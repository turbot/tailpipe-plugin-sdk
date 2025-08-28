package artifact_source

import (
	"context"
	"log/slog"

	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// WithDefaultArtifactSourceConfig sets the default config, e.g. file layout, IF it has not been set from config
// NOTE: in contrast to the artifact config passed to the source from the CLI which is raw HCL which must be parsed,
// the default artifact source config is a ArtifactSourceConfigImpl struct which is populated by the table to set defaults
func WithDefaultArtifactSourceConfig(config *artifact_source_config.ArtifactSourceConfigImpl) row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactSource); ok {
			a.SetDefaultConfig(config)
		}
		return nil
	}
}

// WithArtifactLoader is used to specify an artifact loader
func WithArtifactLoader(loader artifact_loader.Loader) row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactSource); ok {
			a.SetLoader(loader)
		}
		return nil
	}
}

// WithArtifactExtractor is used to specify an artifact extractor
// this is needed if the artifact contains a collection of rows which needs explicit extraction
// (not this is in addition to the default extraction performed by the loaded)
func WithArtifactExtractor(extractor Extractor) row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactSource); ok {
			a.SetExtractor(extractor)
		}
		return nil
	}
}

// WithRowPerLine is used when creating an ArtifactSourceImpl
// it specifies that the row source should treat each line as a separate row
func WithRowPerLine() row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactSource); ok {
			a.SetRowPerLine(true)
		}
		return nil
	}
}

// WithSkipHeaderRow is used when creating an ArtifactSourceImpl
// it specifies that the row source should skip the first row (header row).
func WithSkipHeaderRow() row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactSource); ok {
			// pass empty delimiter
			a.SetSkipHeaderRow()
		}
		return nil
	}
}

// WithHeaderRowNotification is used when creating an ArtifactSourceImpl
// it specifies that the first row of the artifact is a header.
// Use the specified delimiter to split into a list of fields and notify the collector of the header.
// The collector will pass the header columns to all MapRow calls using the `WithHeader` option
func WithHeaderRowNotification(delimiter string) row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactSource); ok {
			a.SetHeaderDelimiter(delimiter)
		}
		return nil
	}
}

// ContentValidator is a function that validates artifact content before processing
// It receives the artifact content (as bytes), source enrichment metadata, and context
// Returns true if the artifact should be processed, false if it should be skipped
type ContentValidator func(ctx context.Context, content []byte, enrichment *schema.SourceEnrichment) bool

// WithContentValidator sets a content validation function for artifacts
// The validator receives artifact content and metadata and returns whether to process the artifact
func WithContentValidator(validator ContentValidator) row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactSource); ok {
			a.SetContentValidator(validator)
		}
		return nil
	}
}

// WithDebugContentValidator wraps a validator with debug logging
// This helps troubleshoot why files are being rejected during validation
func WithDebugContentValidator(validator ContentValidator) row_source.RowSourceOption {
	debugValidator := func(ctx context.Context, content []byte, enrichment *schema.SourceEnrichment) bool {
		sourceLocation := enrichment.ResolveSourceLocation()

		// Log validation attempt with file info
		slog.Info("Debug: Starting content validation",
			"file", sourceLocation,
			"contentSize", len(content))

		// Show content preview (first 200 chars)
		previewSize := min(200, len(content))
		preview := string(content[:previewSize])
		if len(content) > previewSize {
			preview += "..."
		}
		slog.Info("Debug: Content preview", "file", sourceLocation, "preview", preview)

		// Call the actual validator
		isValid := validator(ctx, content, enrichment)

		// Log the result
		slog.Info("Debug: Validation result",
			"file", sourceLocation,
			"valid", isValid,
			"reason", func() string {
				if isValid {
					return "passed validation"
				}
				return "failed validation"
			}())

		return isValid
	}

	return WithContentValidator(debugValidator)
}

// WithNoContentValidation explicitly disables content validation
// Useful for debugging when you want to process all files regardless of content
func WithNoContentValidation() row_source.RowSourceOption {
	return WithContentValidator(func(ctx context.Context, content []byte, enrichment *schema.SourceEnrichment) bool {
		slog.Debug("Content validation disabled, accepting all files", "file", enrichment.ResolveSourceLocation())
		return true
	})
}

// min helper function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
