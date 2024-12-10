package table

import "testing"

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
//			if got := CsvToJsonQuery(tt.args.sourceFile, tt.args.destFile, mappings, tt.args.opts...); got != tt.want {
//				t.Errorf("CsvToJsonQuery() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}

func TestCsvToJsonQuery(t *testing.T) {
	type args struct {
		sourceFile string
		destFile   string
		mappings   map[string]string
		opts       []CsvToJsonOpts
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CsvToJsonQuery(tt.args.sourceFile, tt.args.destFile, tt.args.mappings, tt.args.opts...); got != tt.want {
				t.Errorf("CsvToJsonQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
