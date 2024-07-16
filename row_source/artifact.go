package row_source

import (
	"context"
	"errors"
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/artifact"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"log/slog"
	"path/filepath"
	"sync"
)

// ArtifactRowSource is a RowSource that uses an
// artifact.Source to discover artifacts and an artifact.Extractor to extract rows from those artifacts
type ArtifactRowSource struct {
	Base
	// do we expect the a row to be a line of data
	RowPerLine bool
	Source     artifact.Source
	Loader     artifact.Loader
	Mappers    []artifact.Mapper

	// map of loaders created, keyed by identifier
	// this is populated lazily if we infer the loader from the file type
	loaders    map[string]artifact.Loader
	loaderLock sync.RWMutex

	artifactWg sync.WaitGroup
}

type ArtifactRowSourceOptions func(*ArtifactRowSource)

func WithMapper(mappers ...artifact.Mapper) ArtifactRowSourceOptions {
	return func(a *ArtifactRowSource) {
		a.Mappers = mappers
	}
}
func WithLoader(loader artifact.Loader) ArtifactRowSourceOptions {
	return func(a *ArtifactRowSource) {
		a.Loader = loader
	}
}
func WithRowPerLine() ArtifactRowSourceOptions {
	return func(a *ArtifactRowSource) {
		a.RowPerLine = true
	}
}

func NewArtifactRowSource(artifactSource artifact.Source, opts ...ArtifactRowSourceOptions) (*ArtifactRowSource, error) {
	if artifactSource == nil {
		return nil, fmt.Errorf("artifactSource cannot be nil")
	}
	res := &ArtifactRowSource{
		Source:  artifactSource,
		loaders: make(map[string]artifact.Loader),
	}

	// apply options
	for _, opt := range opts {
		opt(res)
	}

	// add ourselves as observer to res
	err := artifactSource.AddObserver(res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Close implements plugin.RowSource
func (a *ArtifactRowSource) Close() error {
	// close the source
	return a.Source.Close()
}

// Notify implements observable.Observer
// it handles all events which collections may receive
// TODO do we need this in base? depends on how what other implementations need? APISource does not need to be
//
//	an observer? Does Webhook?
func (a *ArtifactRowSource) Notify(event events.Event) error {
	switch e := event.(type) {
	case *events.ArtifactDiscovered:
		// TODO check state to see if we need to download this artifact
		// for now just download

		// increment the wait group - this weill be decremented when the artifact is extracted (or there is an error
		a.artifactWg.Add(1)

		go func() {
			err := a.Source.DownloadArtifact(context.Background(), e.Request, e.Info)
			if err != nil {
				a.artifactWg.Done()
				err := a.NotifyObservers(events.NewErrorEvent(e.Request, err))
				if err != nil {
					slog.Error("Error notifying observers of download error", "download error", err, "notify error", err)
				}
			}
		}()
	case *events.ArtifactDownloaded:
		//extract
		go func() {
			// TODO #err make sure errors handles and bubble back
			err := a.extractArtifact(e)
			// close wait group whether there is an error or not
			a.artifactWg.Done()
			if err != nil {
				err := a.NotifyObservers(events.NewErrorEvent(e.Request, err))
				if err != nil {
					slog.Error("Error notifying observers of extract error", "extract error", err, "notify error", err)
				}
			}
		}()
	default:
		// TODO just ignore?
		return fmt.Errorf("ArtifactRowSource does not handle event type: %T", e)
	}
	return nil
}

// Collect implements plugin.RowSource
// tell our ArtifactRowSource to start discovering artifacts
func (a *ArtifactRowSource) Collect(ctx context.Context, req *proto.CollectRequest) error {
	if err := a.Source.DiscoverArtifacts(ctx, req); err != nil {
		return err
	}
	// now wait for all extractions
	a.artifactWg.Wait()
	return nil
}

func (a *ArtifactRowSource) extractArtifact(e *events.ArtifactDownloaded) error {
	// load artifact data

	// resolve the loader - if one has not been specified, create a default for the file tyoe
	loader, err := a.resolveLoader(e.Info)
	if err != nil {
		return err
	}

	data, err := loader.Load(context.Background(), e.Info)
	if err != nil {
		return fmt.Errorf("error extracting artifact: %w", err)
	}

	slog.Debug("Artifact loader returned", "count", len(data), "artifact", e.Info.Name)

	// now if we have any mappers, call them
	var errList []error
	data, errList = a.mapArtifact(e, data)

	if errCount := len(errList); errCount > 0 {
		return fmt.Errorf("%d %s extracting artifact: %w", errCount, utils.Pluralize("error", errCount), errors.Join(errList...))

	}

	slog.Debug("Artifact mapper returned", "count", len(data), "artifact", e.Info.Name)

	// so data now contains our rows
	// raise row events
	var notifyErrors []error
	for _, row := range data {
		if err := a.NotifyObservers(events.NewRowEvent(e.Request, row, e.Info.EnrichmentFields)); err != nil {
			notifyErrors = append(notifyErrors, err)
		}
	}
	if len(notifyErrors) > 0 {
		return fmt.Errorf("error notifying %d %s of row event: %w", len(notifyErrors), utils.Pluralize("observer", len(notifyErrors)), errors.Join(notifyErrors...))
	}
	return nil
}

func (a *ArtifactRowSource) mapArtifact(e *events.ArtifactDownloaded, data []any) ([]any, []error) {
	var errList []error
	slog.Debug("Mapping artifact", "artifact", e.Info.Name, "data length", len(data), "mapper count", len(a.Mappers))
	for _, m := range a.Mappers {
		var mappedData []any

		slog.Debug("Mapping", "mapper", m.Identifier(), "input count", len(data))
		for _, dataItem := range data {
			mapped, err := m.Map(context.Background(), e.Request, dataItem)
			if err != nil {
				errList = append(errList, err)
			} else {
				mappedData = append(mappedData, mapped...)
			}
		}

		slog.Debug("Mapping complete", "mapper", m.Identifier(), "output count", len(data))

		// replace data with mapped data
		data = mappedData
	}

	return data, errList
}

// resolveLoader resolves the loader to use for the artifact
// - if a loader has been specified, just use that
// - otherwise create a default loader based on the extension
func (a *ArtifactRowSource) resolveLoader(info *types.ArtifactInfo) (artifact.Loader, error) {

	// a loader was specified when rcreating the row source - use that
	if a.Loader != nil {
		return a.Loader, nil
	}

	var key string
	var ctor func() (artifact.Loader, error)
	// figure out which loader to use based on the file extension
	switch filepath.Ext(info.Name) {
	case ".gz":
		if a.RowPerLine {
			key = artifact.GzipRowLoaderIdentifier
			ctor = artifact.NewGzipRowLoader
		} else {
			key = artifact.GzipLoaderIdentifier
			ctor = artifact.NewGzipLoader
		}
	default:
		if a.RowPerLine {
			key = artifact.FileRowLoaderIdentifier
			ctor = artifact.NewFileRowLoader
		} else {
			key = artifact.FileLoaderIdentifier
			ctor = artifact.NewFileLoader
		}
	}

	// have we already created this loader?
	a.loaderLock.RLock()
	l, ok := a.loaders[key]
	a.loaderLock.RUnlock()
	if ok {
		return l, nil
	}

	// upgrade the lock
	a.loaderLock.Lock()
	defer a.loaderLock.Unlock()
	// check the map again
	if l, ok = a.loaders[key]; ok {
		return l, nil
	}

	// so we do need to create
	l, err := ctor()
	if err != nil {
		return nil, err
	}
	a.loaders[key] = l
	return l, nil
}
