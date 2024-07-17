package row_source

import (
	"context"
	"errors"
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/artifact"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/rate_limiter"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"log/slog"
	"path/filepath"
	"sync"
	"time"
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

	// rate limiters
	artifactLoadLimiter *rate_limiter.APILimiter

	artifactWg sync.WaitGroup
}

func NewArtifactRowSource(artifactSource artifact.Source, opts ...ArtifactRowSourceOptions) (*ArtifactRowSource, error) {
	if artifactSource == nil {
		return nil, fmt.Errorf("artifactSource cannot be nil")
	}
	res := &ArtifactRowSource{
		Source:  artifactSource,
		loaders: make(map[string]artifact.Loader),
	}

	// NOTE: see if the source requires a mapper
	// (e.g. if the source is a Cloudwatch source, we need to add a mapper to extract the cloudtrail metadata)
	// NOTE: do this BEFORE applying options so we know there are no mappers set yet
	if mapperFunc := artifactSource.Mapper(); mapperFunc != nil {
		res.Mappers = []artifact.Mapper{mapperFunc()}
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

	res.artifactLoadLimiter = rate_limiter.NewAPILimiter(&rate_limiter.Definition{
		Name: "artifact_load_limiter",
		//FillRate:       5,
		//BucketSize:     5,
		MaxConcurrency: 1,
	})

	return res, nil
}

// Close implements plugin.RowSource
func (a *ArtifactRowSource) Close() error {
	slog.Debug("Closing ArtifactRowSource")
	// close the source
	return a.Source.Close()
}

// Notify implements observable.Observer
// it handles all events which collections may receive
// TODO do we need this in base? depends on how what other implementations need? APISource does not need to be
//
//	an observer? Does Webhook?
func (a *ArtifactRowSource) Notify(ctx context.Context, event events.Event) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	switch e := event.(type) {
	case *events.ArtifactDiscovered:
		// TODO check state to see if we need to download this artifact
		// for now just download

		// increment the wait group - this weill be decremented when the artifact is extracted (or there is an error
		a.artifactWg.Add(1)
		slog.Debug("ArtifactDiscovered - rate limiter waiting", "artifact", e.Info.Name)
		t := time.Now()

		// rate limit the download
		// TODO should we check semaphore outside goroutine but l;imiter inside?
		err := a.artifactLoadLimiter.Wait(ctx)
		if err != nil {
			return fmt.Errorf("error acquiring rate limiter: %w", err)
		}
		slog.Debug("ArtifactDiscovered - rate limiter acquired", "duration", time.Since(t), "artifact", e.Info.Name)

		go func() {
			defer func() {
				a.artifactLoadLimiter.Release()
				slog.Debug("ArtifactDiscovered - rate limiter released", "artifact", e.Info.Name)
			}()

			err = a.Source.DownloadArtifact(context.Background(), e.Info)
			if err != nil {
				a.artifactWg.Done()
				err := a.NotifyObservers(ctx, events.NewErrorEvent(executionId, err))
				if err != nil {
					slog.Error("Error notifying observers of download error", "download error", err, "notify error", err)
				}
			}
		}()
	case *events.ArtifactDownloaded:
		//extract
		go func() {
			// TODO #err make sure errors handles and bubble back
			err := a.extractArtifact(ctx, e)
			// close wait group whether there is an error or not
			a.artifactWg.Done()
			if err != nil {
				err := a.NotifyObservers(ctx, events.NewErrorEvent(executionId, err))
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
	if err := a.Source.DiscoverArtifacts(ctx); err != nil {
		return err
	}
	// now wait for all extractions
	a.artifactWg.Wait()
	return nil
}

// convert a downloaded artifact to a set of raw rows, with optioanl metadata
// invoke the artifact loaded and any configured mapped to convert the artifact to 'raw' rows,
// which are streamed to the enricher
func (a *ArtifactRowSource) extractArtifact(ctx context.Context, e *events.ArtifactDownloaded) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	// load artifact data
	// resolve the loader - if one has not been specified, create a default for the file tyoe
	loader, err := a.resolveLoader(e.Info)
	if err != nil {
		return err
	}

	dataChan := make(chan *artifact.ArtifactData)
	err = loader.Load(context.Background(), e.Info, dataChan)
	if err != nil {
		return fmt.Errorf("error extracting artifact: %w", err)
	}

	count := 0

	// range over the data channel and apply mappers
	for data := range dataChan {
		// pout data into an array as that is what mappers expect
		rows, err := a.mapArtifacts(e, data)

		if err != nil {
			return fmt.Errorf("error mapping artifact: %w", err)
		}
		// raise row events
		var notifyErrors []error
		for _, row := range rows {
			count++
			// NOTE: if no metadata has been set on the row, ase any metadata from the artifact
			if row.Metadata == nil {
				row.Metadata = e.Info.EnrichmentFields
			}
			if err := a.NotifyObservers(ctx, events.NewRowEvent(executionId, row.Data, row.Metadata)); err != nil {
				notifyErrors = append(notifyErrors, err)
			}
		}
		if len(notifyErrors) > 0 {
			return fmt.Errorf("error notifying %d %s of row event: %w", len(notifyErrors), utils.Pluralize("observer", len(notifyErrors)), errors.Join(notifyErrors...))
		}
	}

	slog.Debug("ArtifactRowSource extractArtifact complete", "artifact", e.Info.Name, "rows", count)
	return nil

}

func (a *ArtifactRowSource) mapArtifacts(e *events.ArtifactDownloaded, artifactData *artifact.ArtifactData) ([]*artifact.ArtifactData, error) {
	// mappers may return multiple rows so wrap data in a list
	var dataList = []*artifact.ArtifactData{artifactData}

	// iff there are no mappers, just return the data as is
	if len(a.Mappers) == 0 {
		return []*artifact.ArtifactData{artifactData}, nil
	}
	var errList []error

	// invoke each mapper in turn
	for _, m := range a.Mappers {
		var mappedDataList []*artifact.ArtifactData
		for _, d := range dataList {
			mappedData, err := m.Map(context.Background(), d)
			if err != nil {
				// TODO #err should we give up immediately
				errList = append(errList, err)
			} else {
				mappedDataList = append(mappedDataList, mappedData...)
			}
		}
		// update artifactData list
		dataList = mappedDataList
	}

	if len(errList) > 0 {
		return nil, fmt.Errorf("error mapping artifact rows: %w", errors.Join(errList...))
	}

	return dataList, nil
}

// resolveLoader resolves the loader to use for the artifact
// - if a loader has been specified, just use that
// - otherwise create a default loader based on the extension
func (a *ArtifactRowSource) resolveLoader(info *types.ArtifactInfo) (artifact.Loader, error) {

	// a loader was specified when creating the row source - use that
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
		// yes, return it
		return l, nil
	}

	// no - create and cache a new one
	// upgrade the lock
	a.loaderLock.Lock()
	defer a.loaderLock.Unlock()

	// check the map again (in case of race condition)
	if l, ok = a.loaders[key]; ok {
		return l, nil
	}

	// so we do need to create
	l, err := ctor()
	if err != nil {
		return nil, err
	}
	// store
	a.loaders[key] = l

	return l, nil
}
