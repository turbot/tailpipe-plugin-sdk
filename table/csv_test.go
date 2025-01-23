package table

import (
	_ "github.com/marcboeker/go-duckdb"
	"testing"
)

//
//func TestCsvToJsonQuery(t *testing.T) {
//	type args struct {
//		sourceFile string
//		destFile   string
//		opts       []CsvToJsonOpts
//		mappings   map[string]string
//	}
//	tests := []struct {
//		name string
//		args args
//		want string
//	}{
//		{
//			name: "Default settings",
//			args: args{
//				sourceFile: "data.csv",
//				destFile:   "output.jsonl",
//				opts:       []CsvToJsonOpts{},
//			},
//			want: "COPY (SELECT * FROM read_csv('data.csv', DELIM ',', HEADER TRUE)) TO 'output.jsonl' (FORMAT JSON) RETURNING COUNT(*) AS row_count;",
//		},
//		{
//			name: "Custom delimiter and header off",
//			args: args{
//				sourceFile: "data.csv",
//				destFile:   "output.jsonl",
//				opts: []CsvToJsonOpts{
//					WithCsvDelimiter("\t"),
//					WithCsvHeaderMode(CsvHeaderModeOff),
//				},
//			},
//			want: "COPY (SELECT * FROM read_csv('data.csv', DELIM '\t', HEADER FALSE)) TO 'output.jsonl' (FORMAT JSON) RETURNING COUNT(*) AS row_count;",
//		},
//		{
//			name: "Custom comment character",
//			args: args{
//				sourceFile: "data.csv",
//				destFile:   "output.jsonl",
//				opts: []CsvToJsonOpts{
//					WithCsvComment("#"),
//				},
//			},
//			want: "COPY (SELECT * FROM read_csv('data.csv', DELIM ',', HEADER TRUE, COMMENT '#')) TO 'output.jsonl' (FORMAT JSON) RETURNING COUNT(*) AS row_count;",
//		},
//		{
//			name: "Full schema provided",
//			args: args{
//				sourceFile: "data.csv",
//				destFile:   "output.jsonl",
//				opts: []CsvToJsonOpts{
//					WithCsvSchema(&schema.RowSchema{
//						Columns: []*schema.ColumnSchema{
//							{ ColumnName: "column1"},
//							{ ColumnName: "column2"},
//						},
//						Mode: schema.ModeFull,
//
//					}),
//				},
//			},
//			want: "COPY (SELECT column1, column2 FROM read_csv('data.csv', DELIM ',', HEADER TRUE)) TO 'output.jsonl' (FORMAT JSON) RETURNING COUNT(*) AS row_count;",
//		},
//		{
//			name: "All custom settings",
//			args: args{
//				sourceFile: "data.csv",
//				destFile:   "output.jsonl",
//				opts: []CsvToJsonOpts{
//					WithCsvDelimiter("|"),
//					WithCsvHeaderMode(CsvHeaderModeOff),
//					WithCsvComment(";"),
//					WithCsvSchema(&schema.RowSchema{
//						Columns: []*schema.ColumnSchema{
//							{SourceName: "colA", ColumnName: "columnA"},
//							{SourceName: "colB", ColumnName: "columnB"},
//						},
//					}),
//				},
//			},
//			want: "COPY (SELECT colA AS columnA, colB AS columnB FROM read_csv('data.csv', DELIM '|', HEADER FALSE, COMMENT ';')) TO 'output.jsonl' (FORMAT JSON) RETURNING COUNT(*) AS row_count;",
//		},
//		{
//			name: "Mappings provided with default options",
//			args: args{
//				sourceFile: "data.csv",
//				destFile:   "output.jsonl",
//				mappings: map[string]string{
//					"dest_field1": "source_field1",
//					"dest_field2": "source_field2",
//				},
//				opts: []CsvToJsonOpts{},
//			},
//			want: "COPY (SELECT source_field1 AS dest_field1, source_field2 AS dest_field2 FROM read_csv('data.csv', DELIM ',', HEADER TRUE)) TO 'output.jsonl' (FORMAT JSON) RETURNING COUNT(*) AS row_count;",
//		},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			mappings := tt.args.mappings
//			if mappings == nil {
//				mappings = map[string]string{}
//			}
//			if got := GetReadCsvChunkQueryFormat(tt.args.sourceFile, tt.args.destFile, mappings, tt.args.opts...); got != tt.want {
//				t.Errorf("GetReadCsvChunkQueryFormat() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}

func TestCsvToJsonQuery(t *testing.T) {
	type args struct {
		sourceFile string
		//destFile   string
		//mappings   map[string]string
		opts []CsvToJsonOpts
	}
	var tests []struct {
		name string
		args args
		want string
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetReadCsvChunkQueryFormat(tt.args.sourceFile, tt.args.opts...); got != tt.want {
				t.Errorf("GetReadCsvChunkQueryFormat() = %v, want %v", got, tt.want)
			}
		})
	}
}

//
//// test
//func TestFoo(t *testing.T) {
//
//	// Parameters
//	csvFile := "/Users/kai/tailpipe_data/a.csv"
//	outputTemplate := "/Users/kai/tailpipe_data/output_chunk_%d.jsonl"
//	chunkSize := 7
//	offset := 0
//
//	// Connect to DuckDB
//	db, err := sql.Open("duckdb", "")
//
//	if err != nil {
//		log.Fatalf("Failed to connect to DuckDB: %v", err)
//	}
//	defer db.Close()
//
//	for {
//		// Construct the COPY query
//		outputFile := fmt.Sprintf(outputTemplate, offset/chunkSize)
//		query := fmt.Sprintf(`
//			COPY (
//				SELECT * FROM read_csv('%s', skip=%d)
//				LIMIT %d¡
//			) TO '%s' (FORMAT JSON);
//		`, csvFile, offset, chunkSize, outputFile)
//
//		// Execute the query
//		_, err := db.Exec(query)
//		if err != nil {
//			// Stop if the error indicates we've reached the end of the file
//			if isEndOfFileError(err) {
//				fmt.Println("No more rows to process. Stopping.")
//				break
//			}
//			// Handle other errors
//			log.Fatalf("Failed to execute COPY: %v", err)
//		}
//
//		fmt.Printf("Wrote chunk to %s\n", outputFile)
//
//		// Increment the offset
//		offset += chunkSize
//	}
//
//	fmt.Println("Splitting complete!")
//}
//
//// Helper function to detect "end of file" error
//func isEndOfFileError(err error) bool {
//	var d *duckdb.Error
//	if errors.As(err, &d) {
//		return d.Type == duckdb.ErrorTypeInvalidInput
//	}
//	return false
//}
