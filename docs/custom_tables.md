# Custom tables

## Overview
Custom tables are a means to collect data from arbitrary log files and other sources. 
Supported data formats are:, 
- delimited (csv, tsv)
- jsonl
- any line format for which a regex pattern can be defined 


## Collecting  a custom table
A partition is defined in hcl with 2 labels, the table name and the partition name. 

```hcl
partition "aws_cloudtrail_log" "fs" {
    source "file"  {
        file_layout = ".json.gz"
        paths = ["/Users/kai/tailpipe_data/flaws_cloudtrail_logs"]
    }
}
```

The table may be provided by a plugin (as above), or it may be the of a custom table defined in the config.
In this case there must be a hcl `table` block which matches the table name of the partition.
```hcl
partition "openstack_syslog" "midterms" {
  source "file"  {
    paths = ["/Users/kai/Downloads/OpenStack"]
    file_layout   = ".log$"
  }
}

table "openstack_syslog" {
  format = format.regex.openstack_syslog
  null_value = "-"

  column "tp_timestamp"{
    source = "timestamp"
  }

  column "tp_index" {
    source = "tenant_id"
  }
  column "tp_index2" {
    source = "tenant_id"
    type = "GUID"
  }
}

format "regex" "openstack_syslog"{
  layout = `^(?P<log_file>nova-[\w-]+\.log(?:\.\d+)?\.[\d-]+_[\d:]+)\s+(?P<timestamp>[\d-]+\s+[\d:.]+)\s+(?P<pid>\d+)\s+(?P<log_level>\w+)\s+(?P<component>[\w._-]+)(?:\s+(?:\[(?:req-(?P<request_id>[^\s]+)\s+(?P<user_id>[^\s]+)\s+(?P<tenant_id>[^\s]+)(?:\s+[^\]]+)?|-)]\s+)?(?:(?P<client_ip>\d+\.\d+\.\d+\.\d+)\s+"(?P<http_method>GET|POST|PUT|DELETE)\s+(?P<http_path>[^\s]+)\s+HTTP\/[\d.]+"\s+status:\s+(?P<http_status>\d+)\s+len:\s+(?P<resp_size>\d+)\s+time:\s+(?P<resp_time>[\d.]+)|(?:\[instance:\s+(?P<instance_id>[^\]]+)\])?\s*(?P<message>.*))?)?$`
}
```
NOTE: if a partition name is both the name of a custom table and is also provided by a plugin, then *the custom
table will be used*.

## Custom Table definition

Custom tables are defined in a `table` block. Table blocks have a single label, which is the name of the table. They define the schema and processing rules for your data, specifying how to parse and transform your source data into a queryable format.

The format of the source data is defined by the `format` property, which must reference either a format block defined in the config or a format preset defined by a plugin.

Example:
```hcl
table "openstack_syslog" {
  format = format.regex.openstack_syslog
  null_value = "-"

  column "tp_timestamp"{
    source = "timestamp"
  }

  column "tp_index" {
    source = "tenant_id"
  }
}
```

The schema of a custom table is determined as follows:
- if the table property `automap_source_fields` is set `false` (the default), then only columns defined in 
the table block will be included in the table.
- if `automap_source_fields` is set to `true`, then all source fields will be included in the table,
and any additional columns defined in the table block will be added to the table. 

Note - column definitions can be used for multiple purposes:
- to specify a column to include in this table which does not exist in the source data. 
For this usage, either the `source` property is used specify which field in the source field to use, 
or the `transform` property should be used to specify a duckdb expression or function to use to produce a value for 
the column.
- a special case of the above is to provide the source data mapping for the tailpipe `tp_` fields. Of these, 
it is required that a mapping is provided for at least the `tp_timestamp` field, and ideally, for the `tp_index` field.
- to specify the data type of a column which exists in the source data. By default, DuckDB type inference is used to 
determine the type of a column base on the source data type, but the provides as mechanism to control.
  (for example, in cases where a field usually looks like a GUID but is not always a GUID, it may be useful to 
specify a string type - otherwise if DuckDB infers the type as a GUID, then any non-GUID values will result in an error)
in the source data to use.

### Custom table HCL reference

#### Table properties
- `format` - the default format of the source data. THis must refer to either a formaty block defined in the config, 
or a `format preset` defined by a plufin (see [Source data format] for details
- `null_value` - the value which is translated ads null when it occurs in the source data. For example, 
  if the null value is "-", then any column in the source data which has a value of "-" will be translated to NULL in the resulting parquet file.
- `automap_source_fields` - if set to `true`, then all fields from the source data will be included in the table, 
  and any additional columns defined in the table block will be added to the table. 
  If set to `false` (or not set), then only columns defined in the table block will be included in the table.
- `exclude_source_fields` - a list of source fields to exclude from the table. This is only used if `automap_source_fields` is set to `true`.
- Column blocks
  - `type` - the type of the column. IIf the column is required, the type is optional as DuckDB will infer the type from the source data.
  If the column is optional, then the type must be specified.
  - `source` - (optional) the fields in the source data to use for this column.
  - `description` - (optional) a description of the column. This is used to generate documentation for the table.
  - `required` - (optional) if set to `true`, then the column is required and a validation error will be raised if the column is not present in the source data.
  - `null_value` - (optional) overrides the table level null value for a specific column.
  



## Source data format
The data `format` defines both the format of the input data and the parsing mechanism which should be used.

The properties of the format are specific to the format type

Formats types are implemented by plugins. A number of formats types are provided by the `core` plugin.

Instances of a format type can be defined in a `format` block. Also, plugins may export format `presets` which may be referenced by name. 
These may be discovered using the introspection commands: `tailpipe plugin show <plugin name>` or `tailpipe format list`

<MORE DETAILS/examples>

A `table` block may define a default format by settining its `format` property to reference either a format defined in
config or a format preset defined by a plugin.

A `source` block may specify a format to be used with the data for that specific source 
(e.g. a file source containing csv files may specify the `delimited` format).  


### Core Plugin Formats

#### Grok Format
The Grok format is used for parsing log lines using Grok patterns, which are a way to parse log lines into structured data.

Properties:
- `layout` (required): The Grok pattern that defines how to parse the log line
- `patterns` (optional): A map of custom Grok patterns that can be referenced in the layout
- `description` (optional): A description of the format

Example:
```hcl
format "grok" "custom_log" {
  layout = "%{TIMESTAMP_ISO8601:time_local} - %{NUMBER:event_id} - %{WORD:user} - [%{DATA:location}] \"%{DATA:message}\" %{WORD:severity}"
  patterns = {
    "REGION" = "[a-zA-Z0-9\\-]+"
  }
}
```

#### Regex Format
The Regex format is used for parsing log lines using regular expressions with named capture groups.

Properties:
- `layout` (required): The regular expression pattern with named capture groups
- `description` (optional): A description of the format

Example:
```hcl
format "regex" "custom_log" {
  layout = `^(?P<timestamp>[\d-]+\s+[\d:.]+)\s+(?P<pid>\d+)\s+(?P<log_level>\w+)\s+(?P<component>[\w._-]+)`
}
```

#### Delimited Format
The Delimited format is used for parsing CSV, TSV, and other delimited file formats. The properties are passed directly to DuckDB which implements the delimited data parsing.

Properties:
- `all_varchar` (optional): Skip type detection and assume all columns are VARCHAR
- `allow_quoted_nulls` (optional): Allow conversion of quoted values to NULL
- `decimal_separator` (optional): The decimal separator for numbers
- `delimiter` (optional): The character that separates columns
- `escape` (optional): The escape character for quoted values
- `filename` (optional): Include an extra filename column
- `force_not_null` (optional): List of columns that should not be converted to NULL
- `header` (optional): Whether the file contains a header row
- `ignore_errors` (optional): Ignore parsing errors
- `max_line_size` (optional): Maximum line size in bytes
- `new_line` (optional): New line character(s) ('\r', '\n', or '\r\n')
- `normalize_names` (optional): Remove non-alphanumeric characters from column names
- `null_padding` (optional): Pad missing columns with NULL values
- `null_str` (optional): String(s) that represent NULL values
- `quote` (optional): The quoting character
- `sample_size` (optional): Number of sample rows for auto-detection
- `timestamp_format` (optional): Format for parsing timestamps
- `description` (optional): A description of the format

Example:
```hcl
format "delimited" "custom_csv" {
  delimiter = ","
  header = true
  null_str = "-"
}
```

#### JSONL Format
The JSONL (JSON Lines) format is used for parsing JSON data where each line is a valid JSON object.

Properties:
- `description` (optional): A description of the format

Example:
```hcl
format "jsonl" "custom_jsonl" {
  description = "JSON Lines format with one JSON object per line"
}
```
