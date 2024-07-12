package row_source

import (
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/artifact"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

// ArtifactRowSource is a RowSource that uses an
// artifact.Source to discover artifacts and an artifact.Extractor to extract rows from those artifacts
type ArtifactRowSource struct {
	Base
	ArtifactSource     artifact.Source
	ArtifactExtractors []artifact.Extractor
}

func NewArtifactRowSource(artifactSource artifact.Source, artifactExtractors ...artifact.Extractor) (*ArtifactRowSource, error) {
	if artifactSource == nil {
		return nil, fmt.Errorf("artifactSource cannot be nil")
	}
	if len(artifactExtractors) == 0 {
		return nil, fmt.Errorf("at least one artifactExtractor must be provided")
	}

	source := &ArtifactRowSource{
		ArtifactSource:     artifactSource,
		ArtifactExtractors: artifactExtractors,
	}

	sourceExtractor := artifactExtractors[0]
	sinkExtractor := artifactExtractors[len(artifactExtractors)-1]
	var mapperExtractors []artifact.Extractor
	if len(artifactExtractors) > 2 {
		mapperExtractors = artifactExtractors[1 : len(artifactExtractors)-1]
	}
	// validate first mapper is a source, last is a sink and any middle ones are mappers
	if err := validateExtractors(sourceExtractor, sinkExtractor, mapperExtractors); err != nil {
		return nil, err
	}

	// setup observation events
	artifactSource.AddObserver(sourceExtractor)
	if len(artifactExtractors) > 1 {
		for i := 1; i < len(artifactExtractors); i++ {
			artifactExtractors[i-1].AddObserver(artifactExtractors[i])
		}
	}
	sinkExtractor.AddObserver(source)

	return source, nil
}

// Close implements plugin.RowSource
func (a *ArtifactRowSource) Close() error {
	// close the source
	return a.ArtifactSource.Close()
}

// validate first mapper is a source, last is a sink and any middle ones are mappers
func validateExtractors(sourceExtractor artifact.Extractor, sinkExtractor artifact.Extractor, mapperExtractors []artifact.Extractor) error {
	if _, isSource := sourceExtractor.(artifact.ExtractorSource); !isSource {
		return fmt.Errorf("first extractor must be a source")
	}
	if _, isSink := sinkExtractor.(artifact.ExtractorSink); !isSink {
		return fmt.Errorf("last extractor must be a sink")
	}
	for i, extractor := range mapperExtractors {
		if _, isMapper := extractor.(artifact.ExtractorMapper); !isMapper {
			return fmt.Errorf("extractor %d is not a mapper", i)
		}
	}
	return nil
}

// Notify implements observable.Observer
// it handles all events which collections may receive
// TODO do we need this in base? depends on how what other implementations need? APISource does not need to be
//
//	an observer? Does Webhook?
func (a *Base) Notify(event events.Event) error {
	switch e := event.(type) {
	case *events.Row:
		return a.OnRow(e.Request, e.Row, e.EnrichmentFields)
	default:
		// TODO just ignore?
		return fmt.Errorf("ArtifactRowSource does not handle event type: %T", e)
	}
}

// Collect implements plugin.RowSource
// tell our ArtifactRowSource to start discovering artifacts
func (a *ArtifactRowSource) Collect(ctx context.Context, req *proto.CollectRequest) error {
	return a.ArtifactSource.DiscoverArtifacts(ctx, req)
}
