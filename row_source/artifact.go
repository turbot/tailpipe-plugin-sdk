package row_source

import (
	"context"
	"errors"
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/artifact"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"log/slog"
	"sync"
)

// ArtifactRowSource is a RowSource that uses an
// artifact.Source to discover artifacts and an artifact.Extractor to extract rows from those artifacts
type ArtifactRowSource struct {
	Base
	Source     artifact.Source
	Loader     artifact.Loader
	Mappers    []artifact.Mapper
	artifactWg sync.WaitGroup
}

func NewArtifactRowSource(artifactSource artifact.Source, loader artifact.Loader, mappers ...artifact.Mapper) (*ArtifactRowSource, error) {
	if artifactSource == nil {
		return nil, fmt.Errorf("artifactSource cannot be nil")
	}
	if loader == nil {
		return nil, fmt.Errorf("loader cannot be nil")
	}

	res := &ArtifactRowSource{
		Source:  artifactSource,
		Loader:  loader,
		Mappers: mappers,
	}

	// add ourselves as observer to res
	artifactSource.AddObserver(res)

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
				a.NotifyObservers(events.NewErrorEvent(e.Request, err))
			}
		}()
	case *events.ArtifactDownloaded:
		//extract
		go func() {
			// TODO #err make sure errors handles and bubble back
			err := a.extractArtifact(e)
			a.artifactWg.Done()

			if err != nil {
				a.NotifyObservers(events.NewErrorEvent(e.Request, err))
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
	data, err := a.Loader.Load(context.Background(), e.Info)
	if err != nil {
		// TODO raise error event
		a.NotifyObservers(events.NewErrorEvent(e.Request, err))
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
	for _, row := range data {
		a.NotifyObservers(events.NewRowEvent(e.Request, row, e.Info.EnrichmentFields))
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
