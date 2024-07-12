package artifact

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"log/slog"
	"os"
)

// define enume for extraction strategy
type ExtractionStrategy int

const (
	// Extract the entire object from the artifact
	ExtractObject ExtractionStrategy = iota
	// Extract rows from the artifact
	ExtractRows
)

// GzipExtractorSource is an ExtractorSource that can extracts an object from a gzip file
type GzipExtractorSource struct {
	ExtractorSourceBase
	strategy ExtractionStrategy
}

func NewGzipExtractorSource(source Source, strategy ExtractionStrategy) *GzipExtractorSource {
	return &GzipExtractorSource{
		ExtractorSourceBase: NewExtractorSourceBase(source),
		strategy:            strategy,
	}
}

// Notify implements observable.Observer
func (g GzipExtractorSource) Notify(event events.Event) error {
	switch e := event.(type) {
	case *events.ArtifactDiscovered:
		// just download immediately (for now)
		return g.Source.DownloadArtifact(context.Background(), e.Request, e.Info)
	case *events.ArtifactDownloaded:
		// call ExtractArtifact (
		return g.ExtractArtifact(context.Background(), e.Request, e.Info)
	default:
		slog.Warn("GzipExtractorSource received unexpected event type: %T", e)
	}
	return nil
}

// ExtractArtifact implements ExtractorSource
// Extracts an object from a gzip file
func (g GzipExtractorSource) ExtractArtifact(ctx context.Context, req *proto.CollectRequest, info *types.ArtifactInfo) error {
	inputPath := info.Name
	gzFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("error opening %s: %w", inputPath, err)
	}
	defer gzFile.Close()

	gzReader, err := gzip.NewReader(gzFile)
	if err != nil {
		return fmt.Errorf("error creating gzip reader for %s: %w", inputPath, err)
	}
	defer gzReader.Close()

	// now depending on the extraction strategy, we may need to decode the object or send line by line
	switch g.strategy {
	case ExtractObject:
		return g.extractObject(ctx, req, info, gzReader)
	case ExtractRows:
		return g.extractRows(ctx, req, info, gzReader)
	default:
		return fmt.Errorf("unknown extraction strategy: %d", g.strategy)
	}
}

func (g GzipExtractorSource) extractObject(ctx context.Context, req *proto.CollectRequest, info *types.ArtifactInfo, gzReader *gzip.Reader) error {
	var item any
	if err := json.NewDecoder(gzReader).Decode(&item); err != nil {
		return fmt.Errorf("error decoding %s: %w", info.Name, err)
	}

	// we do not know how to get rows from the unzipped artifact - just raise an ArtifactExtracted event
	// and let the next extractor in the chain handle it
	artifact := types.NewArtifact(info, item)

	return g.OnArtifactExtracted(req, artifact)
}

func (g GzipExtractorSource) extractRows(ctx context.Context, req *proto.CollectRequest, info *types.ArtifactInfo, gzReader *gzip.Reader) error {
	scanner := bufio.NewScanner(gzReader)

	for scanner.Scan() {
		// check context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// get the lin eof text
		line := scanner.Text()
		// put into an artifact
		a := types.NewArtifact(info, line)
		// call base OnArtifactExtracted
		err := g.OnArtifactExtracted(req, a)
		if err != nil {
			return fmt.Errorf("error notifying observers of extracted artifact: %w", err)
		}
	}
	return nil
}
