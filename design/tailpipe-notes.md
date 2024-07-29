

# Sources vs  Collections


  - **Connections** provide *credentials* and *configuration options* to connect to external services.  Tailpipe connections are similar to connections in other pipelings like Steampipe and Flowpipe.

  - **Sources** are *mechanism* for getting logs.  Usually, a source will connect to a resource via a **connection** -- the connection specifies the credentials and account scope.   The source is usually more specific than the connection though;  a *connection* provides the ability to interact with an AWS account, and you may have several sources that use that connection but provide logs from different services and locations - perhaps an aws s3 source in one bucket, another aws s3 source in another bucket, and a 3rd source that get logs from a Cloudwatch Logs log group.

  The source is responsible for:
  - initiating file transfers / requests
  - downloading / copying raw data
  - unzipping/untaring, etc
  - incremental transfer / tracking, retrying, etc
  - source-specific filtering for sources that support them - eg [Cloudwatch log filters](https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_FilterLogEvents.html)

  The source does NOT:
   - modify or standardize the data (the collection does that)
   - generalized filtering based on row contents (the collection does that)


  There are built-in sources and sources defined in plugins?
    - S3 is defacto object store standard
      - BUT who resolves credentials / connections?  If s3 support is built in, then connections/creds must be handled by tailpipe, not the plugin...


  

  - **Collections**  translate sources into a standardized format, stored in partitions.  

  A collection will accept rows from a single **source**, optionally filter and/or standardize ane write to a "refined" row-wise file (JSON, CSV, etc), then will read those files and convert to parquet files stored in a specific filesystem structure.

  The collection is responsible for:
   - modifying and standardizing the data from the *source*
   - generalized filtering based on row contents
   - writing parquet files
   - lifecycle management of the storage - automated tiering, deletion, etc


  The collection  does NOT:
    - transfer files or interact with external systems directly (the source does)
    - Create or manage views and tables


  Collections are typed, and the first label specifies the type.  
  ```h
  // #1
  collection "aws_cloudtrail_log" "production" { 
    ...
  }

  // or #2 ?
  collection "prod_trail" { 
      type = "aws_cloudtrail_log"
  }

  // #2 may be better for "custom" tables?
  collection "my-stuff" { 
      type = table.my_table
  }

  ```
    - the types are plugin specific - the `aws` plugin know about the `aws_cloudtrail_log` type - it knows how to read various source formats and defines the "canonical" format (the table structure).
      -  what if multiple plugins have the same type name?   do they "register" them when the plugin is installed?

    - the plugin(?) will (automatically) create a table with the same name as the type.  The data for all collections of that type that are part of the schema will be returned by that table.

  - plugin ?  could hav collisions????
      - in steampipe, each connection has its OWN schema, so collision are less important (but bc search_path, it still matters....)



 Each collection has a single "source" ?
  - i don't see a technical reason why a collection couldn't have multiple sources?
  

  ```h
  source "aws_cloudwatch" "prod_trails" {
    connection  = connection.aws.prod
    log_streams = ["aws-cloudtrail-logs/123456789012_CloudTrail_*"]

  }

  source  "aws_s3" "prod_trails"{
    connection  = connection.aws.prod
    paths      = ["s3://my-prod-trails/AWSLogs/*/CloudTrail/*"] 
  }

  source "aws_s3" "dev_trails"{
    connection  = connection.aws.dev
    paths      = ["s3://my-dev-log-bucket/AWSLogs/*/CloudTrail/*"] 
  }



  collection "aws_cloudtrail_log" "production_cloudwatch" { 
    source = source.aws_cloudwatch.prod_trails
  }
    
  collection "aws_cloudtrail_log" "production_s3" { 
    source = source.aws_s3.prod_trails

  }

  collection "aws_cloudtrail_log" "dev_s3" { 
    source = source.aws_s3.dev_trails
  }
  ```

- as sub-block ?
  - the sources are pretty specific to a single collection anyway - the "shared" info (creds, etc) is mostly in the connection anyway?

  ```h
  collection "aws_cloudtrail_log" "production_cloudwatch" { 
    source "aws_cloudwatch" {
      connection  = connection.aws.prod
      log_streams = ["aws-cloudtrail-logs/123456789012_CloudTrail_*"]

    }
  }
    
  collection "aws_cloudtrail_log" "production_s3" { 

    source  "aws_s3" {
      connection  = connection.aws.prod
      paths      = ["s3://my-prod-trails/AWSLogs/*/CloudTrail/*"] 
    }
  }


  collection "aws_cloudtrail_log" "dev_s3" { 

    source "aws_s3" {
      connection  = connection.aws.dev
      paths      = ["s3://my-dev-log-bucket/AWSLogs/*/CloudTrail/*"] 
    }

  }
  ```

- type as arg instead of label?
  - works better for "custom" tables / types ? or do you HAVE to create a plugin to create a new "type"/table?

```h
collection "dev_s3" { 
  type = table.aws_cloudtrail_log 
  //type = "aws_cloudtrail_log"
  //table = table.aws_cloudtrail_log
  //table = "aws_cloudtrail_log"
  
  
  source "aws_s3" {
    connection  = connection.aws.dev
    paths      = ["s3://my-dev-log-bucket/AWSLogs/*/CloudTrail/*"] 
  }

}
```



collection with variable input format that the user must define????
  - eg flow logs....
  ```h
  collection "aws_flowlog" "dev_s3" { 

    source "aws_s3" {
      connection  = connection.aws.dev
      paths      = ["s3://my-dev-log-bucket/AWSLogs/*/CloudTrail/*"] 
    }
  
    //non default format... 
    // i think many people would think  this is a property of the source instead of the collection..

    // format in vpc flowlogs style (as it appears in the console?)
    // this sytax is specific to aws vpc flow logs...
    source_format = "${version} ${vpc-id} ${subnet-id} ${interface-id} ${instance-id}" ///... etc...

    // vs generic format directives?
    source_format = {
      type              = "delimited" // vs json, jsonl, etc....
      delimiter         = "\s"        // other args are format type specific ?
      null_column_char  =  "-"
      fields = [
        "timestamp",
        "version",
        "vpc-id",
        "subnet-id",
        "interface-id",
        "instance-id",
        "account-id",
        "type",
        "srcaddr",
        "dstaddr",
        "srcport",
        "dstport",
        "pkt-srcaddr",
        "pkt-dstaddr",
        "protocol",
        "bytes",
        "packets",
        "start",
        "end",
        "action",
        "tcp-flags",
        "log-status"
      ]
    }
  
  }
  ```

  vs
  ```h
  collection "aws_flowlog" "dev_s3" { 

    source "aws_s3" {
      connection  = connection.aws.dev
      paths      = ["s3://my-dev-log-bucket/AWSLogs/*/CloudTrail/*"] 
    }

    source_format = format.my_flowlogs
  }


  // vs generic format directives?
  format "my_flowlogs" {
    type              = "delimited" // vs json, jsonl, etc....
    delimiter         = "\s"        // other args are format type specific ?
    null_column_char  =  "-"
    fields = [
      "timestamp",
      "version",
      "vpc-id",
      "subnet-id",
      "interface-id",
      "instance-id",
      "account-id",
      "type",
      "srcaddr",
      "dstaddr",
      "srcport",
      "dstport",
      "pkt-srcaddr",
      "pkt-dstaddr",
      "protocol",
      "bytes",
      "packets",
      "start",
      "end",
      "action",
      "tcp-flags",
      "log-status"
    ]
  }
  

  ```

  even though its not really the source that knows the format, its seems logical from an HCL perspective that the "source" is what has specific format/schema/structure??  would make multipe sources per collection straightforward from an hcl perspective?



  ```h
  collection "aws_flowlog" "all" { 

    source "aws_s3" {
      connection  = connection.aws.prod
      paths       = ["s3://my-prod-log-bucket/AWSLogs/*/CloudTrail/*"] 
      format      = format.my_flowlogs_prod
    }

    source "aws_s3" {
      connection  = connection.aws.dev
      paths       = ["s3://my-dev-log-bucket/AWSLogs/*/CloudTrail/*"] 
      format      = format.my_flowlogs_dev
    }
  
  }
  ```



  A *partition* is place to store/read the "canonicalized" parquet files.  You can define multiple partitions for a collection if you want to do things like tier them by age (would need to also create `schema`s that account for this....) 

  ```h
  collection "my-other stuff" { 
    type = table.my_custom_table

    source {
      type        = "cloudwatch"
      connection  = connection.aws.dev
      log_streams = ["my-logs/*"] 
      format      = format.jsonl.my_netlogs_csv
    }

    // local filesystem for new entries
    partition {
      from  = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-168h"))
      to    = formatdate("YYYY-MM-DD", timestamp()) // default
      path = "/tp/mystuff/current"
    }

    // remote object store for older entries
    partition {
      path = "s3://my-archive-bucket"
      from  = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-365d"))
      to   = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-168h"))
    }
  ```


- A **Table** represents a duckdb relation (is it a view or a table?).

plugins automatically create a table for each type they define.
  - can you override them?

A user may also create their own, as for a custom log file....

```h

  table "my_custom_table" {
    column "timestamp" {
      type = "datetime"
    }

    column "src_port" {
      type = "int"
    }

    column "src_ip" {
      type = "cidr"
    }

    column "dest_port" {
      type = "int"
    }

    column "dest_ip" {
      type = "cidr"
    }

    column "protocol" {
      type = "varchar"
    }
  }


  collection "my-stuff" { 
    type = table.my_custom_table

    source "aws_s3" {
      connection  = connection.aws.dev
      paths      = ["s3://my-dev-log-bucket/AWSLogs/*/CloudTrail/*"] 
    }

    // if format is jsonl or json and source keys match the column names, then dont need to specify `field_map`
    // if format is delimited and column order matches the table order, then dont need to specify `fields`

    // jsonl example...
    source_format = {
      type       = "jsonl"

      field_map  = {
        timestamp  = "ts"
        src_ip     = "from_addr"
        src_port   = "from_port"
        dest_ip    = "to_addr"
        dest_port  = "to_port"
        protocol   = "proto"        
      }
    }

    // csv example
    source_format = {
      type              = "delimeted"
      delimiter         = ","        // other args are format type specific ?
      fields            = [ timestamp, src_ip, dest_ip, src_port, dest_port, protocol ]       
    }
  }


```



collection with custom transform???? via flowpipe? via script?

```h
  collection "mystuff" { 
    type = table.my_custom_table

    source "command" {
      command  = "flowpipe pipeline run import_my_flowlogs"
    }
  
  }
```



lifecycle policies to expire date, or tier to s3, or remove from 'working set' tables?


- A **Schema** represents a duckdb schema.  

Unlike schemas in steampipe that are tied to a single connection for a single plugin, schemas in tailpipe may include tables from all tables, all connections, all collections, across all plugins.  Tailpipe schemas provide a filtered view based on the 

By default, a single `tailpipe` schema exists that contains all tables with all data from all collections.  Essentially:
```h
schema "all" {
    types = ["*"] // not required, default
    collections = ["*"] // not required, default
    connections = ["*"] // not required, default
}
```


You may also create your own custom schemas to provide a custom (but consistent) view of the tables (both implicit and custom tables).

```h
schema "account_123456789012" {
    //should `types` be `tables` ?
    types       = ["*"] // not required, default 
    collections = ["*"] // not required, default
    connections = ["123456789012"] // not required, default
}

schema "recent" {
    types       = ["*"] // default
    collections = ["*"] // default
    connections = ["*"] // default
    start_date  = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-168h"))
    end_date    = formatdate("YYYY-MM-DD", timestamp()) // default
}

schema "aws_q1_materialized" {
    types        = ["aws_*"]
    start_date   = "2024-01-01"
    end_date     = "2024-03-31"
    materialized = true   // what does this mean for a schema?
}

```
-----


are there `destination`s separate from sources?
  - what about the `intermediate` files (json, csv)
  - are `source` and `destination` really different?
    - source types are likely a superset of destinations
  - `destination` is perhaps misleading?  It's the destination for the files when you import, but its the source for the table.... 





-----




----
    - sources include 
  - Formats/schemas are the *structure* of the logs
  - Different sources for the same type of data may have different source formats/schemas, but these get resolved to a single schema (collection?) in tailpipe.
    - ex:  CLoud Trail logs..
      - the sources may be S3, file, cloudwatch logs, API/http
      - The format/schema/collection is aws_cloudtrail_event.  The aws_cloudwatch_event table consildates the logs from *any* sources that contdain cloudtrail logs in a standardized format







```bash
ls ~/src/tailpipe/dest/aws_cloudtrail_log/tp_collection\=default/tp_connection\=811596193553/tp_year\=2020/tp_month\=9/tp_day\=22/file_e52ccd80-551a-47e3-8ff2-2a86713482bc.parquet 
/Users/jsmyth/src/tailpipe/dest/aws_cloudtrail_log/tp_collection=default/tp_connection=811596193553/tp_year=2020/tp_month=9/tp_day=22/file_e52ccd80-551a-47e3-8ff2-2a86713482bc.parquet
MacBook-Pro:source_files jsmyth$ 
```

```bash
MacBook-Pro:source_files jsmyth$ tree ~/src/tailpipe/dest
/Users/jsmyth/src/tailpipe/dest
└── aws_cloudtrail_log
    └── tp_collection=default
        └── tp_connection=811596193553
            ├── tp_year=2017
            │   ├── tp_month=10
            │   │   ├── tp_day=1
            │   │   │   └── file_0e55415e-625f-4683-a1ea-510a2330a283.parquet
            │   │   ├── tp_day=10
            │   │   │   └── file_97d22e40-5cd7-4ac3-838a-66e68e2e9a67.parquet
            │   │   ├── tp_day=11
            │   │   │   └── file_6b6174ca-1b2f-4e36-a06c-aca2125083b7.parquet
```

format assumes one connection per collection....
  - is connection that important in the storage structure?   
    - The account/subs/project from which you fetch the logs (the `connection` of the source) is not necessarily related to the account/subs/project to which the log records belong
    - in nathans design the collection had only a single source, thus a single connection per collection which would make this folder redundant
    - if we did multiple sources per collection, this level would make sense, but perhaps `tp_source` would be more appropriate than `tp_connection` ?

- table
  - collection
    - connection ??????







---

currently:

row source

a collection is fed a row_source, which recieves row_events 


row_sources are typed:
cloud_trail row_events are json - the xsource deserializes
vpc flow logs are text



source streams raw row to collection
collection writes json
the CLI writes the parquet....



--


collections 
  - separate the collection process from the storage???
  - who "owns" the "canonical" db format (parquet format, view in duckdb)





table with multiple collections, collection with multiple sources and  partitions 

  ```h

  table "my_custom_table" {
    column "timestamp" {
      type = "datetime"
    }

    column "src_port" {
      type = "int"
    }

    column "src_ip" {
      type = "cidr"
    }

    column "dest_port" {
      type = "int"
    }

    column "dest_ip" {
      type = "cidr"
    }

    column "protocol" {
      type = "varchar"
    }
  }


  collection "my-stuff" { 
    type = table.my_custom_table

    source {
      type = "aws_s3"
      connection = connection.aws.dev
      paths      = ["s3://my-dev-log-bucket/netlogs/*"] 
      format     = format.jsonl.my_netlogs_csv
    }

    source {
      type = "aws_s3"
      connection = connection.aws.dev
      paths      = ["s3://my-dev-log-bucket/csvlgs/*net.csv"] 
      format     = format.jsonl.my_netlogs_csv
    }

    partition {
      from  = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-168h"))
      to    = formatdate("YYYY-MM-DD", timestamp()) // default
      path = "/tp/mystuff/current"
    }

    partition {
      path = "s3://my-archive-bucket"
      from  = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-365d"))
      to   = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-168h"))
    }


  }

  format "jsonl" "my_netlogs_jsonl" {
    field_map  = {
      timestamp  = "ts"
      src_ip     = "from_addr"
      src_port   = "from_port"
      dest_ip    = "to_addr"
      dest_port  = "to_port"
      protocol   = "proto"        
    }
  }

  // csv example
  format  "csv" "my_netlogs_csv" {
    fields            = [ timestamp, src_ip, dest_ip, src_port, dest_port, protocol ]       
  }




  collection "my-other stuff" { 
    type = table.my_custom_table

    source {
      type        = "cloudwatch"
      connection  = connection.aws.dev
      log_streams = ["my-logs/*"] 
      format      = format.jsonl.my_netlogs_csv
    }

    partition {
      from  = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-168h"))
      to    = formatdate("YYYY-MM-DD", timestamp()) // default
      path = "/tp/mystuff/current"
    }

    partition {
      path = "s3://my-archive-bucket"
      from  = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-365d"))
      to   = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-168h"))
    }

  ```




- partition as top-level object?   multiple collections per partition??


```h
  collection "my-stuff" { 
    type = table.my_custom_table

    source {
      type = "aws_s3"
      connection = connection.aws.dev
      paths      = ["s3://my-dev-log-bucket/netlogs/*"] 
      format     = format.jsonl.my_netlogs_csv
    }

    source {
      type = "aws_s3"
      connection = connection.aws.dev
      paths      = ["s3://my-dev-log-bucket/csvlgs/*net.csv"] 
      format     = format.jsonl.my_netlogs_csv
    }

    //  do you specify paritions on a per-collection basis?
    // or are the partitions global and implicitly used?
    partitions = [partition.recent, partition.archive]
  }


  partition "recent" {
    from  = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-168h"))
    to    = formatdate("YYYY-MM-DD", timestamp()) // default
    path = "/tp/mystuff/current"
  }


  partition "archive" {
    path = "s3://my-archive-bucket"
    from = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-365d"))
    to   = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-168h"))
  }

  ```





- storage as top-level object?   multiple collections per partition??


```h
  collection "my-stuff" { 
    type = table.my_custom_table

    source {
      type = "aws_s3"
      connection = connection.aws.dev
      paths      = ["s3://my-dev-log-bucket/netlogs/*"] 
      format     = format.jsonl.my_netlogs_csv
    }

    source {
      type = "aws_s3"
      connection = connection.aws.dev
      paths      = ["s3://my-dev-log-bucket/csvlgs/*net.csv"] 
      format     = format.jsonl.my_netlogs_csv
    }

    store =  store.default
  }


  store "default" {
    partition "recent" {
      from  = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-168h"))
      to    = formatdate("YYYY-MM-DD", timestamp()) // default
      path = "/tp/mystuff/current"

    }


    partition "archive" {
      path = "s3://my-archive-bucket"
      from = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-365d"))
      to   = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-168h"))
    }


    // maybe partition with 'where` syntax on any tp_ column?
    partition "archive" {
      path = "s3://my-archive-bucket"

      // dates really need support for current_date / now(), data functions (interval,etc)
      where  = "tp_timestamp > current_date - interval '10' day"
      
      // but archiving specific years would be easy
      // where  = "tp_year = 2023"
      
      // The non-date tp_ fields are probably not particularly interesting for partitioning though?
              // tp_source_type = pipes_audit_log
              // tp_source_name = 
              // tp_source_location = 
              // tp_ingest_timestamp = 1721058633404
              // tp_timestamp = 1697769820000
              // tp_source_ip = 
              // tp_destination_ip = 
              // tp_collection = pipes_audit_log
              // tp_connection = pipes.turbot.com:turbot-ops
              // tp_year = 2023
              // tp_month = 10
              // tp_day = 20
              // tp_akas = [o_cdr4fqhp6032stn2t0dg]
              // tp_ips = 
              // tp_tags = 
              // tp_domains = 
              // tp_emails = 
              // tp_usernames = [vkumbha] 
      
    }
  }

  ```







  Schemas and partitions are probably related..... ??
  do want to partition each table individually?  do the lifecycles vary by schema, or table, or both?


  
```h
schema "account_123456789012" {
    //should `types` be `tables` ?
    types       = ["*"] // not required, default 
    collections = ["*"] // not required, default
    connections = ["123456789012"] // not required, default
}

schema "recent" {
    types       = ["*"] // default
    collections = ["*"] // default
    connections = ["*"] // default
    start_date  = formatdate("YYYY-MM-DD", timeadd(timestamp(), "-168h"))
    end_date    = formatdate("YYYY-MM-DD", timestamp()) // default
}

schema "aws_q1_materialized" {
    types        = ["aws_*"]
    start_date   = "2024-01-01"
    end_date     = "2024-03-31"
    materialized = true   // what does this mean for a schema?
}

  -------




```bash
# refresh all collections
tailpipe collect

# refresh a specific collecion, all sopurces
tailpipe collect my_collection

# refresh a specific source from a specific collecion
tailpipe collect my_collection my_s3_cloudtrail_source

tailpipe collection list
tailpipe collection my_collection source list


```


```bash
# list all collections
tailpipe collection list

# Refresh a collection
tailpipe collection collecetion_name collect

tailpipe collection collecetion_name source list

tailpipe collection collecetion_name source source_name list


```



```bash
tailpipe query
tailpipe query "select * from aws_cloudtrail"
tailpipe query "select * from aws_cloudtrail" --output json # pretty, plain, json, jsonl, csv
```


```bash
tailpipe connection list
tailpipe connection show as_prod_01
```




-----




- how data is physically laid out 
  - @nathan had laid it by "connection", but the intention wasn't the `connection` from which the files were transferred, but rather the "accountable" to (from?) which the entries (file???) originated

- how can we bring the "working set" concept forward?
  - Data Lake style storage is the default concept.....
    - But ingested tailpipe data is not a data lake:
      - The data is structured
      - it has a schema when it is written


  - but you want to "download" or cache a given working set while you are working on it.
    - this may be a manual, intentional action, or automatic, or combo of both ?
      - should you "pull" and "check out" a dataset, version control style? 
        - Checkout to update also?
        - Implies the dataset is fixed.... not pulled via dynamic rules each time ?
        - could "version" it too?  pull it, but "check out" an earlier version?
          - unlike git, you don't want EVERYTHING to be downloaded though... 

        - Or is a one-time "query" / "pull" that is dynamic / flexible?

      - are there "summary datasets" or "catalogs" - to allow you to locate the sources that may be interesting? or provide "rolled up" view? 
        - parquet files contain metadata, and you can add your own custom key-value metadata 


    - What native duckdb capabilities exist that may influence the design?  
        - [Native S3 API](https://duckdb.org/docs/extensions/httpfs/s3api.html), GCS support sith [httpfs extension](https://duckdb.org/docs/extensions/httpfs/overview)
        - [hive partitioning](https://duckdb.org/docs/data/partitioning/hive_partitioning.html) with automatic [filter pushdown](https://duckdb.org/docs/data/partitioning/hive_partitioning.html#filter-pushdown)
        - [partial read](https://duckdb.org/docs/extensions/httpfs/https.html#partial-reading) for parquet files on S3


    - potentially means separation the collection process from the query source in the HCL????
      - ie you could have multiple tailpipe instances ingesting source data and writing to s3, and multiple tailpipe instances querying different sets of data (this is kind of like separating storage and compute in Snowflake....)


- what are the aspects that define a working set - what are the useful "partitions"?
  - by age seems obvious...
  - by account/project/sub?  or groups of them (dev, prod, qa, etc)?
  - only include specific tables


- The storage layout and tiering of the data impacts the ability to efficiently query it
- While it is completely possible to decouple the view of the data from the storage, the storage of the data will affect the performance



```bash
MacBook-Pro:source_files jsmyth$ tree ~/src/tailpipe/dest
/Users/jsmyth/src/tailpipe/dest
└── aws_cloudtrail_log
    └── tp_collection=default
        └── tp_connection=811596193553
            ├── tp_year=2017
            │   ├── tp_month=10
            │   │   ├── tp_day=1
            │   │   │   └── file_0e55415e-625f-4683-a1ea-510a2330a283.parquet
            │   │   ├── tp_day=10
            │   │   │   └── file_97d22e40-5cd7-4ac3-838a-66e68e2e9a67.parquet
            │   │   ├── tp_day=11
            │   │   │   └── file_6b6174ca-1b2f-4e36-a06c-aca2125083b7.parquet
```


partition on any tp_ field, but default to a standard like:
```h
partition "remote" {
  partition_by =  ["tp_collection", "tp_connection", "tp_year", "tp_month", "tp_day" ]
  location     = "s3://my-bucket/tailpipe/store"
  connection   = connection.aws.my_prod_account
}

// can customize default, but otherwise defaults to local files
partition "default" {
  partition_by =  ["tp_collection", "tp_connection", "tp_year", "tp_month", "tp_day" ]
  location     = "~/.tailpipe/store"
}




  collection "aws_cloudtrail_log" "production_cloudwatch" { 
    source "aws_cloudwatch" {
      connection  = connection.aws.prod
      log_streams = ["aws-cloudtrail-logs/123456789012_CloudTrail_*"]
    }

    // if no partition defined then uses partition.default.  


  }
    
  collection "aws_cloudtrail_log" "production_s3" { 

    source  "aws_s3" {
      connection  = connection.aws.prod
      paths      = ["s3://my-prod-trails/AWSLogs/*/CloudTrail/*"] 
    }

    partition = partition.remote
  }


  dataset "default" {
    partition    = "default"
    types        = ["*"]
  }


  dataset "all" {
    partition    = "default"
    types        = ["*"]
    //start_date   = "2024-01-01"
    //end_date     = "2024-03-31"
    //materialized = true   
  }

```


rename `tp_connection` ?  `tp_account` ??  `tp_cloud_account`?
  - note that this is a per-row item of information in the source, not per file!!!


---
formalize HCL that we have thus far

- do we even need the plugin to query locally once we have the data collected in parquet



----