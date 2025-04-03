package table

import (
	"fmt"
	"testing"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

func TestGetTempTableQuery(t *testing.T) {
	testCases := []struct {
		name          string
		format        formats.Format
		sourceFile    string
		columns       []string
		schema        *schema.TableSchema
		expectedQuery string
		expectedError bool
	}{
		{
			name:       "JSONL with schema",
			format:     &formats.JsonLines{},
			sourceFile: "test.jsonl",
			columns:    []string{"id", "name", "timestamp"},
			schema: &schema.TableSchema{
				AutoMapSourceFields: true,
			},
			expectedQuery: `-- Create temp table from source data
create temp table temp_data as
select *
from read_json('test.jsonl');

-- Return the columns as an array
select string_agg(name, ',') from pragma_table_info('temp_data');`,
			expectedError: false,
		},
		{
			name:       "JSONL with tp_index mapping",
			format:     &formats.JsonLines{},
			sourceFile: "test.jsonl",
			columns:    []string{"id", "name", "timestamp", "account_id"},
			schema: &schema.TableSchema{
				AutoMapSourceFields: true,
				Columns: []*schema.ColumnSchema{
					{
						SourceName:  "account_id",
						ColumnName:  "tp_index",
						Description: "Mapped tp_index",
					},
				},
			},
			expectedQuery: `-- Create temp table from source data
create temp table temp_data as
select *
from read_json('test.jsonl');

-- Return the columns as an array
select string_agg(name, ',') from pragma_table_info('temp_data');`,
			expectedError: false,
		},
		{
			name:       "CSV with schema",
			format:     &formats.Delimited{},
			sourceFile: "test.csv",
			columns:    []string{"id", "name", "timestamp"},
			schema: &schema.TableSchema{
				AutoMapSourceFields: true,
			},
			expectedQuery: `-- Create temp table from source data
create temp table temp_data as
select *
from read_csv('test.csv', delim ',', header true);

-- Return the columns as an array
select string_agg(name, ',') from pragma_table_info('temp_data');`,
			expectedError: false,
		},
		{
			name:       "CSV without auto-map",
			format:     &formats.Delimited{},
			sourceFile: "test.csv",
			columns:    []string{"id", "name", "timestamp"},
			schema: &schema.TableSchema{
				AutoMapSourceFields: false,
			},
			expectedQuery: `-- Create temp table from source data
create temp table temp_data as
select *
from read_csv('test.csv', delim ',', header true);

-- Return the columns as an array
select string_agg(name, ',') from pragma_table_info('temp_data');`,
			expectedError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query, err := getTempTableQuery(tc.sourceFile, tc.format)

			if tc.expectedError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if query != tc.expectedQuery {
				t.Errorf("Expected query:\n%s\nGot query:\n%s", tc.expectedQuery, query)
			}
		})
	}
}

func TestGetCopyQuery(t *testing.T) {

	// Get current timestamp for comparison
	currentTime := time.Now()

	testCases := []struct {
		name          string
		format        formats.Format
		columns       []string
		schema        *schema.TableSchema
		expectedQuery string
		expectedError bool
	}{
		{
			name:    "JSONL with schema",
			format:  &formats.JsonLines{},
			columns: []string{"id", "name", "timestamp"},
			schema: &schema.TableSchema{
				AutoMapSourceFields: true,
			},
			expectedQuery: fmt.Sprintf(`-- Transform and copy data to destination
copy (select
    "id",
    "name",
    "timestamp",
    'test_table' as tp_table,
    'test_partition' as tp_partition,
    gen_random_uuid() as tp_id,
    '%s' as tp_ingest_timestamp,
    case
		when tp_timestamp is not null
		then date_trunc('day', tp_timestamp::timestamp)
	end as tp_date,
    coalesce(tp_index, 'default') as tp_index
from temp_data)
to 'test.jsonl' (
    format json
);

-- Get row count
select count(*) as row_count from temp_data;`, currentTime.Format(time.RFC3339)),
			expectedError: false,
		},
		{
			name:    "JSONL with tp_index mapping",
			format:  &formats.JsonLines{},
			columns: []string{"id", "name", "timestamp", "account_id"},
			schema: &schema.TableSchema{
				AutoMapSourceFields: true,
				Columns: []*schema.ColumnSchema{
					{
						SourceName:  "account_id",
						ColumnName:  "tp_index",
						Description: "Mapped tp_index",
					},
				},
			},
			expectedQuery: fmt.Sprintf(`-- Transform and copy data to destination
copy (select
    "account_id" as "tp_index",
    "account_id",
    "id",
    "name",
    "timestamp",
    'test_table' as tp_table,
    'test_partition' as tp_partition,
    gen_random_uuid() as tp_id,
    '%s' as tp_ingest_timestamp,
    case
		when tp_timestamp is not null
		then date_trunc('day', tp_timestamp::timestamp)
	end as tp_date
from temp_data)
to 'test.jsonl' (
    format json
);

-- Get row count
select count(*) as row_count from temp_data;`, currentTime.Format(time.RFC3339)),
			expectedError: false,
		},
		{
			name:    "CSV with schema",
			format:  &formats.Delimited{},
			columns: []string{"id", "name", "timestamp"},
			schema: &schema.TableSchema{
				AutoMapSourceFields: true,
			},
			expectedQuery: fmt.Sprintf(`-- Transform and copy data to destination
copy (select
    "id",
    "name",
    "timestamp",
    'test_table' as tp_table,
    'test_partition' as tp_partition,
    gen_random_uuid() as tp_id,
    '%s' as tp_ingest_timestamp,
    case
		when tp_timestamp is not null
		then date_trunc('day', tp_timestamp::timestamp)
	end as tp_date,
    coalesce(tp_index, 'default') as tp_index
from temp_data)
to 'test.jsonl' (
    format json
);

-- Get row count
select count(*) as row_count from temp_data;`, currentTime.Format(time.RFC3339)),
			expectedError: false,
		},
		{
			name:    "CSV without auto-map",
			format:  &formats.Delimited{},
			columns: []string{"id", "name", "timestamp"},
			schema: &schema.TableSchema{
				AutoMapSourceFields: false,
			},
			expectedQuery: fmt.Sprintf(`-- Transform and copy data to destination
copy (select
    'test_table' as tp_table,
    'test_partition' as tp_partition,
    gen_random_uuid() as tp_id,
    '%s' as tp_ingest_timestamp,
    case
		when tp_timestamp is not null
		then date_trunc('day', tp_timestamp::timestamp)
	end as tp_date,
    coalesce(tp_index, 'default') as tp_index
from temp_data)
to 'test.jsonl' (
    format json
);

-- Get row count
select count(*) as row_count from temp_data;`, currentTime.Format(time.RFC3339)),
			expectedError: false,
		},
		{
			name:    "CSV with column transform",
			format:  &formats.Delimited{},
			columns: []string{"id", "name", "timestamp"},
			schema: &schema.TableSchema{
				AutoMapSourceFields: true,
				Columns: []*schema.ColumnSchema{
					{
						ColumnName:  "name",
						Transform:   "upper(name)",
						Description: "Transformed name to uppercase",
					},
				},
			},
			expectedQuery: fmt.Sprintf(`-- Transform and copy data to destination
copy (select
    upper(name) as "name",
    "id",
    "timestamp",
    'test_table' as tp_table,
    'test_partition' as tp_partition,
    gen_random_uuid() as tp_id,
    '%s' as tp_ingest_timestamp,
    case
		when tp_timestamp is not null
		then date_trunc('day', tp_timestamp::timestamp)
	end as tp_date,
    coalesce(tp_index, 'default') as tp_index
from temp_data)
to 'test.jsonl' (
    format json
);

-- Get row count
select count(*) as row_count from temp_data;`, currentTime.Format(time.RFC3339)),
			expectedError: false,
		},
		{
			name:    "CSV with time format",
			format:  &formats.Delimited{},
			columns: []string{"id", "name", "timestamp"},
			schema: &schema.TableSchema{
				AutoMapSourceFields: true,
				Columns: []*schema.ColumnSchema{
					{
						ColumnName:  "tp_timestamp",
						SourceName:  "timestamp",
						TimeFormat:  `%Y-%m-%dT%H:%M:%S%z`,
						Description: "Timestamp with specific format",
					},
				},
			},
			expectedQuery: fmt.Sprintf(`-- Transform and copy data to destination
copy (select
    strptime("timestamp", '%%Y-%%m-%%dT%%H:%%M:%%S%%z') as "tp_timestamp",
    "id",
    "name",
    "timestamp",
    'test_table' as tp_table,
    'test_partition' as tp_partition,
    gen_random_uuid() as tp_id,
    '%s' as tp_ingest_timestamp,
    case
		when tp_timestamp is not null
		then date_trunc('day', tp_timestamp::timestamp)
	end as tp_date,
    coalesce(tp_index, 'default') as tp_index
from temp_data)
to 'test.jsonl' (
    format json
);

-- Get row count
select count(*) as row_count from temp_data;`, currentTime.Format(time.RFC3339)),
			expectedError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query := getCopyQuery("test_table", "test_partition", "test.jsonl", tc.columns, tc.schema, currentTime)

			if query != tc.expectedQuery {
				t.Errorf("Expected query:\n%s\nGot query:\n%s", tc.expectedQuery, query)
			}
		})
	}
}
