# Custom tables

A custom table can be defined using a hcl `table` block. 
The input schema (i.e. source data format) is defined in a `format` block, which is referenced by the `source` block.
The output schema (i.e. the table schema) is defined in the `table` block.

All custom tables are provided by the `custom` plugin. This does not define any tables, but contains the logic needed 
to collect from custom tables, based on `table` hcl definitions.

## Input schema vs output schema

The 'input schema' defines the format and structure of the data in the source file. 
This is defined in a `format` block, which the `source` config should reference. 
For delimited and JSON formats, the input schema is be inferred from the (first) source file, 
and the format block just specifies the parsing options. 
For other log formats the input schema is defined using grok/regex syntax (tbd).  

The 'output schema' defines the schema of the tailpipe table. This is defined by the table `columns` blocks. 
Note: this includes the mapping of input to output fields.  
 
QUESTION: does the format define columns mappings/ columns to exclude/column types? (NO - for now that is in the table)


## Example: Csv log file config 
```hcl

# define a partition on the my_log custom table
partition "my_csv_log" "test"{
    source "file_system"  {
        paths = ["/Users/kai/tailpipe_data/logs"]
        extensions = [".csv"]
        
        # format MUST be set for a custom table
        format = format.csv_logs
    }  
}

# define a custom table 'my_log'
table  "my_csv_log" {
    
    # specify a partial list of columns to include in the table:
    # - specify mapping from source field using the 'source' property - note this must correspond to a field
    #   TODO what happens is a mapped field is missing - do we need a concept of required/optional?
    # - set the column type using the 'type' property 
    # - all other field fromn the source will be included with their original names and inferred types
    
    
    # enrichment fields
    # a mapping MUST be provided for tp_timestamp
    column "tp_timestamp" {
        source =  "time_local"
    }
    # optionally, provide mappings for other enrichment fields
    column "tp_index" {
        source = "account_id"
    }
    
    // provide definitons for any other columns which require name mappings or we want to specify a type
    column "org_id" {
        # rename the org_id field to org
        source = "org"
    }
    
    column "user_id" {
        # ensure this field is a varchar
        type = "varchar"
    }
}

format "csv_logs" {
    # delimited | json | jsonl | grok | regex 
    type              = "delimited"
    
    # properties specific to the format
    delimiter         = "\t"
    header            = false
    # other (duck db derived) options with defaults shown
    # escape            = ""
    # quote             = ""
    # nullstring        = "NULL"
    # encoding          = "UTF-8"
    # all_varchar       = false
    # force_not_null    = []
    # force_null       = []
    # force_quote       = []
    # date_format       = ""
    # timestamp_format  = ""
    # compression       = ""
           
}

```



## Example: Custom log file defined with grok 
```hcl

# define a partition on the my_log custom table
partition "my_log" "test"{
    source "file_system"  {
        paths = ["/Users/kai/tailpipe_data/logs"]
        extensions = [".log"]
        
        # format MUST be set for a custom table
        format = format.custom_logs
    }  
}

# table as before

format "custom_logs" {
    type              = "grok"`
    pattern = "%{TIMESTAMP_ISO8601:time_local} account_id=%{NUMBER:account_id} org=%{DATA:org} user_id=%{NUMBER:user_id} \[%{LOGLEVEL:log_level}\] %{GREEDYDATA:message}"    
}

```









