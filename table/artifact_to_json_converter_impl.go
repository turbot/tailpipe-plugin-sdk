package table

import (
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
)

type ArtifactToJsonConverterImpl[S parse.Config] struct {
}

func (c *ArtifactToJsonConverterImpl[S]) GetSourceMetadata(_ S) []*SourceMetadata[*DynamicRow] {
	return []*SourceMetadata[*DynamicRow]{
		{
			SourceName: constants.ArtifactSourceIdentifier,
			// set a null loader so we don't receive row events - instead we implement ArtifactToJsonConverter
			// to convert the artifact to JSONL directly
			Options: []row_source.RowSourceOption{artifact_source.WithArtifactLoader(artifact_loader.NewNullLoader())},
		},
	}
}

func (c *ArtifactToJsonConverterImpl[S]) EnrichRow(_ *DynamicRow, _ S, _ enrichment.SourceEnrichment) (*DynamicRow, error) {
	// this should never be called as we are using a null loader which means we will not receive row events
	panic("EnrichRow should never be called for tables implementing ArtifactToJsonConverter")
}

//func (c *ArtifactToJsonConverterImpl[S]) CsvToJSONL(ctx context.Context, sourceFile, executionId string, chunkNumber int, opts []CsvToJsonOpts) (int, int, error) {
//	// get the query format - we will render this with the row offset for each chunk
//	queryFormat := GetReadCsvChunkQueryFormat(sourceFile, opts...)
//
//	var rowCount int
//	offset := 0
//	chunkIdx := 0
//
//	// connect to DuckDB
//	db, err := sql.Open("duckdb", "")
//	if err != nil {
//		return 0, 0, fmt.Errorf("failed to open DuckDB connection: %w", err)
//	}
//	defer db.Close()
//
//	for {
//		//cConstruct the output file path
//		absChunkIdx := chunkNumber + chunkIdx
//		outputFile := ExecutionIdToFileName(executionId, absChunkIdx)
//		// TODO get inbox path
//		// build select query - format with the offset
//		selectQuery := fmt.Sprintf(queryFormat, offset)
//		query := fmt.Sprintf(`
//			COPY (
//				%s
//			) TO '%s' (FORMAT JSON);
//		`, selectQuery, outputFile)
//
//		// execute the query
//		_, err := db.Exec(query)
//		if err != nil {
//			// stop if the error indicates we've reached the end of the file
//			if isEndOfFileError(err) {
//				fmt.Println("No more rows to process. Stopping.")
//				// get the row count from the previous
//				prevFilename := ExecutionIdToFileName(executionId, absChunkIdx-1)
//				finalChunkRowCount, err := c.getRowCount(prevFilename, db)
//				if err != nil {
//					return 0, 0, fmt.Errorf("failed to get row count: %w", err)
//				}
//				rowCount = (chunkIdx-1)*JSONLChunkSize + finalChunkRowCount
//				break
//			}
//			return 0, 0, fmt.Errorf("failed to execute query: %w", err)
//		}
//
//		// TODO raise event
//
//		// increment the offset and chunk index
//		offset += JSONLChunkSize
//		chunkIdx++
//
//	}
//
//	return chunkIdx, rowCount, nil
//}
//
//func (c *ArtifactToJsonConverterImpl[S]) getRowCount(filename string, db *sql.DB) (int, error) {
//	//select row count from json
//	rowCountQuery := fmt.Sprintf(`SELECT COUNT(*) FROM (SELECT * FROM read_json('%s'))`, filename)
//	rowCount, err := db.Query(rowCountQuery)
//	if err != nil {
//		return 0, fmt.Errorf("failed to get row count: %w", err)
//	}
//	defer rowCount.Close()
//	var count int
//	for rowCount.Next() {
//		err := rowCount.Scan(&count)
//		if err != nil {
//			return 0, fmt.Errorf("failed to scan row count: %w", err)
//		}
//	}
//	return 0, nil
//}
//
//
//// Helper function to detect "end of file" error
//func isEndOfFileError(err error) bool {
//	var d *duckdb.Error
//	if errors.As(err, &d) {
//		return d.Type == duckdb.ErrorTypeInvalidInput
//	}
//	return false
//}
//
//func (c *ArtifactToJsonConverterImpl[S]) GetArtifactConversionQuery(_, _ string, _ S) (string, error) {
//	panic("getArtifactConversionQueryFormat must be implemented by struct embedding ArtifactToJsonConverterImpl")
//}
