# Content Validation Feature

The `ValidateContent` feature allows you to validate the full content of artifact files **before** any mapper or options processing begins. This enables you to:

- Skip invalid files early without wasting processing resources
- Validate file structure, headers, or content patterns
- Make validation decisions based on both content and metadata
- Implement custom business logic for file acceptance

## How It Works

1. **Early Validation**: Content validation happens immediately after file download but before any loader, mapper, or row processing
2. **Full File Content**: The validator receives the complete file content as bytes, not line-by-line
3. **Skip on Failure**: If validation returns `false`, the file is skipped entirely with appropriate logging
4. **Rich Context**: Validator has access to both file content and source enrichment metadata

## Basic Example

```go
package main

import (
    "context"
    "strings"

    "github.com/turbot/tailpipe-plugin-sdk/artifact_source"
    "github.com/turbot/tailpipe-plugin-sdk/schema"
    "github.com/turbot/tailpipe-plugin-sdk/table"
)

// ValidateCostUsageReportContent validates AWS Cost Usage Report files
func ValidateCostUsageReportContent(ctx context.Context, content []byte, enrichment *schema.SourceEnrichment) bool {
    contentStr := string(content)

    // Check if this looks like a valid Cost Usage Report
    lines := strings.Split(contentStr, "\n")
    if len(lines) < 1 {
        return false
    }

    // Validate CSV headers
    firstLine := strings.ToLower(lines[0])
    expectedHeaders := []string{
        "line_item_usage_account_id",
        "line_item_line_item_type",
        "line_item_usage_start_date",
        "line_item_product_code",
    }

    // Require at least 3 out of 4 expected headers
    headerCount := 0
    for _, header := range expectedHeaders {
        if strings.Contains(firstLine, header) {
            headerCount++
        }
    }

    return headerCount >= 3
}

// Table configuration with content validation
func (t *CostUsageReportTable) GetSourceMetadata() ([]*table.SourceMetadata[*CostUsageReport], error) {
    return []*table.SourceMetadata[*CostUsageReport]{
        {
            SourceName: "aws_s3_bucket",

            // ValidateContent is called with FULL file content before any processing
            ValidateContent: ValidateCostUsageReportContent,

            Mapper: NewCostAndUsageReportMapper(),
            Options: []row_source.RowSourceOption{
                artifact_source.WithRowPerLine(),
                artifact_source.WithHeaderRowNotification(","),
            },
        },
    }, nil
}
```

## Advanced Example with Metadata

```go
// Advanced validator using both content and metadata
func ValidateWithMetadataContext(ctx context.Context, content []byte, enrichment *schema.SourceEnrichment) bool {
    sourceLocation := enrichment.ResolveSourceLocation()

    // Skip test files
    if strings.Contains(sourceLocation, "/test/") || strings.Contains(sourceLocation, "/dev/") {
        return false
    }

    // Validate minimum file size
    if len(content) < 100 {
        return false // Skip very small files
    }

    // Content validation based on metadata
    if partition, exists := enrichment.Metadata["partition"]; exists {
        contentStr := string(content)

        if strings.HasPrefix(partition, "BILLING_PERIOD=") {
            // CUR 2.0 format - validate accordingly
            return strings.Contains(contentStr, "line_item_usage_account_id")
        } else if strings.Contains(partition, "date=") {
            // Cost optimization format - different validation
            return strings.Contains(contentStr, "cost_category") ||
                   strings.Contains(contentStr, "recommendation")
        }
    }

    // Validate file format by extension
    if strings.HasSuffix(sourceLocation, ".csv") {
        // Basic CSV validation
        contentStr := string(content)
        lines := strings.Split(contentStr, "\n")
        return len(lines) > 1 && strings.Contains(lines[0], ",")
    }

    return true
}

// Validation for specific file patterns
func ValidateReportType(ctx context.Context, content []byte, enrichment *schema.SourceEnrichment) bool {
    contentStr := string(content)

    // Determine expected content based on file name
    fileName := enrichment.ResolveSourceLocation()

    switch {
    case strings.Contains(fileName, "cost-usage"):
        return validateCostUsageReport(contentStr)
    case strings.Contains(fileName, "billing"):
        return validateBillingReport(contentStr)
    case strings.Contains(fileName, "usage"):
        return validateUsageReport(contentStr)
    default:
        // Unknown file type, skip
        return false
    }
}

func validateCostUsageReport(content string) bool {
    requiredFields := []string{
        "line_item_usage_account_id",
        "line_item_product_code",
        "line_item_usage_type",
    }

    for _, field := range requiredFields {
        if !strings.Contains(content, field) {
            return false
        }
    }
    return true
}
```

## Performance Considerations

- **Early Exit**: Invalid files are rejected before expensive processing begins
- **Memory Efficient**: Full file content is read once for validation, then normal streaming processing continues
- **Logging**: Failed validations are logged at INFO level with artifact name
- **No Resource Waste**: Skipped files don't consume mapper, loader, or row processing resources

## Migration from Line-by-Line Validation

If you were previously doing validation during row processing, you can now move it to content validation:

```go
// OLD: Validation during row processing (inefficient)
func (m *MyMapper) Map(ctx context.Context, row any, opts ...MapOption) (*MyRow, error) {
    // Validate each row individually
    if !isValidRow(row) {
        return nil, fmt.Errorf("invalid row")
    }
    // ... mapping logic
}

// NEW: Validation before any processing (efficient)
func ValidateFileContent(ctx context.Context, content []byte, enrichment *schema.SourceEnrichment) bool {
    // Validate entire file structure at once
    return isValidFile(string(content))
}

// Use in table configuration
{
    SourceName: "my_source",
    ValidateContent: ValidateFileContent, // Called once per file
    Mapper: NewMyMapper(), // Only called for valid files
    Options: [...],
}
```

## Processing Flow

1. **Artifact Discovery**: Files are discovered based on layout patterns
2. **Immediate Download & Processing**: Each artifact is processed **immediately** after download (not batched)
3. **Content Validation**: `ValidateContent` function is called with decompressed, full file content
4. **Processing Decision**:
   - If validation returns `true`: Continue with loader → mapper → row processing
   - If validation returns `false`: Skip file entirely, log skip reason, move to next file
5. **Row Extraction**: Successfully processed files increment the "Extracted" count
6. **Resource Cleanup**: Temporary files are cleaned up regardless of validation result

## Debugging Processing Issues

If you see downloads but 0 extractions, check the logs for:

- **Content validation failures**: `Content validation failed, skipping artifact`
- **Empty files**: `Artifact processed but no rows extracted`
- **Processing errors**: Look for error messages during artifact processing

### Common Issues:

1. **ValidateContent rejecting all files**: Check your validation logic
2. **Compressed files**: Content is automatically decompressed (.gz, .zip, .zst)
3. **Empty or malformed files**: Files may download but contain no valid data
4. **Loader issues**: Wrong file format or loader configuration

### Example Debug ValidateContent:

```go
func DebugValidateContent(ctx context.Context, content []byte, enrichment *schema.SourceEnrichment) bool {
    sourceLocation := enrichment.ResolveSourceLocation()

    // Log validation attempt
    slog.Info("Validating content",
        "file", sourceLocation,
        "size", len(content),
        "preview", string(content[:min(100, len(content))]))

    // Your validation logic here
    isValid := strings.Contains(string(content), "expected_header")

    slog.Info("Validation result", "file", sourceLocation, "valid", isValid)
    return isValid
}
```

This ensures maximum efficiency by processing files immediately after download while providing rich context for validation decisions and debugging.
