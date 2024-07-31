# Configuring Sources

## Overview

A collection has a single source, which has a type and a name, e.g.
```hcl

  source "aws_s3_bucket" "dev_trails"{
    connection  = connection.aws.dev
    paths      = ["s3://my-dev-log-bucket/AWSLogs/*/CloudTrail/*"] 
  }

  source "gcp_storage_bucket" "prod_trails"{
    connection  = connection.aws.prod
    paths      = ["gcp://my-dev-log-bucket/AWSLogs/*/CloudTrail/*"] 
  }

  collection "aws_cloudtrail_log" "production_cloudwatch" { 
    source = source.aws_cloudwatch.prod_trails
  }
```


## Source Types
In the SDK, the 'source' is provided by the interface `RowSource`. This has a `Collect` method that starts the collectin of rows.
The RowSource with raise a `Row` event for each `raw` row it reads. (A raw row is a row as read from the source with no 
enrichment applied. Som mapping/processing may be applied by the RowSource to ensure the raw row is in the format expected by the Collection)

### ArtifactRowSource
The `ArtifactRowSource` is a `RowSource` that discovers/download `artifacts` from a source. It applies processing to the 
artifacts to extract rows from them.

ArtifactRowSource has an `artifact_source.Source` which is responsible for discovering and downloading the artifacts.

In the example above, the sources shown are in fact _artifact sources_, as opposed to _RowSources_. 

The code to configure a source for a collection must handle this, and treat the name of artifact sources as a shortcut for creating an `ArtifactRowSource` with the specified `artifact_source.Source`.  
