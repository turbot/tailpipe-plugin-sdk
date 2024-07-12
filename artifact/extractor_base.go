package artifact

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// ExtractorBase is a base struct which should be embedded in all extractors
type ExtractorBase struct {
	observable.Base
}

// ExtractorSourceBase is a base struct for source extractors
// it implements event raising for artifact extraction
type ExtractorSourceBase struct {
	ExtractorBase

	// extractor source talks to the source
	Source Source
}

func NewExtractorSourceBase(source Source) ExtractorSourceBase {
	return ExtractorSourceBase{
		Source: source,
	}
}

// OnArtifactExtracted indicates an artifact has been (partially?) extracted but the rows have not been extracted
func (s ExtractorSourceBase) OnArtifactExtracted(req *proto.CollectRequest, a *types.Artifact) error {
	err := s.NotifyObservers(events.NewArtifactExtractedEvent(req, a))

	if err != nil {
		return fmt.Errorf("error notifying observers of extracted artifact: %w", err)
	}
	return nil
}

// TODO this is unused/untested as yet
// ExtractorMapperBase is a base struct for mapper extractors - it returns true for IsMapper
type ExtractorMapperBase struct {
	ExtractorBase
}

// ExtractorSinkBase is a base struct for sink extractors - it returns true for IsSink
// it implements event raising for row extraction
type ExtractorSinkBase struct {
	ExtractorBase
}

// OnRow is called by the sink when it has a row to send
func (s ExtractorSinkBase) OnRow(req *proto.CollectRequest, row any, enrichmentFields *enrichment.CommonFields) error {
	err := s.NotifyObservers(events.NewRowEvent(req, row, enrichmentFields))

	if err != nil {
		return fmt.Errorf("error notifying observers of extracted artifact: %w", err)
	}
	return nil
}

// TODO this is unused/untested as yet
// ExtractorSourceSinkBase is a base struct for source AND sink extractors - it returns true for IsSource and IsSink
type ExtractorSourceSinkBase struct {
	ExtractorBase
	// extractor sourcesink talks to the source
	Source Source
}

func NewExtractorSourceSinkBase(source Source) ExtractorSourceSinkBase {
	return ExtractorSourceSinkBase{
		Source: source,
	}
}

// OnArtifactExtracted indicates an artifact has been (partially?) extracted but the rows have not been extracted
func (s ExtractorSourceSinkBase) OnArtifactExtracted(req *proto.CollectRequest, a *types.Artifact) error {
	// just call OnRow
	return s.OnRow(req, a.Data, a.EnrichmentFields)
}

// OnRow is called by the sink when it has a row to send
func (s ExtractorSourceSinkBase) OnRow(req *proto.CollectRequest, row any, enrichmentFields *enrichment.CommonFields) error {
	err := s.NotifyObservers(events.NewRowEvent(req, row, enrichmentFields))

	if err != nil {
		return fmt.Errorf("error notifying observers of extracted artifact: %w", err)
	}
	return nil
}
