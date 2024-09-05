# Download Testing 

Comparison testing of:
- Tailpipe - using max_concurrency of both 10 and 16 to match competitors
- aws s3 sync (CLI) - has max_concurrency 10 by default
- seqsense/s3sync - has max_concurrency 16 by default [https://github.com/seqsense/s3sync]

## One Day
- Bucket: `aws-cloudtrail-logs-165086849490-3b37f8dd`
- Prefix: `AWSLogs/o-z3cf4qoe7m/287590803701/CloudTrail/us-east-1/2024/07/31/` Download: `18372KB`
- Prefix: `AWSLogs/o-z3cf4qoe7m/287590803701/CloudTrail/us-east-2/2024/07/31/` Download: `270940KB`

### Tailpipe

**Concurrency 10 us-east-1**
- Command: `time tailpipe collect --config-path /Users/graza/.tailpipe/ --collection aws_cloudtrail_log.aws_cloudtrail_s3`
- Time: 
  - artifacts discovered: 1004, artifacts downloaded: 1004, artifacts extracted: 1004, rows enriched: 54539, rows converted: 54539, errors: 0
  - Timing (may overlap):
    - discovery: 17.590391s
    - download:  17.41608s
    - extract:   17.243186s
    - enrich:    17.240229s

**Concurrency 16 us-east-1**
- Command: `time tailpipe collect --config-path /Users/graza/.tailpipe/ --collection aws_cloudtrail_log.aws_cloudtrail_s3`
- Time: 5.93s user 1.54s system 36% cpu 20.694 total
  - artifacts discovered: 1004, artifacts downloaded: 1004, artifacts extracted: 1004, rows enriched: 54539, rows converted: 54539, errors: 0
  - Timing (may overlap):
    - discovery: 11.480979s
    - download:  10.83075s
    - extract:   10.682046s
    - enrich:    10.676414s

**Concurrency 16 us-east-2 NO PROCESSING OF FILES**
- Command: `time tailpipe collect --config-path /Users/graza/.tailpipe/ --collection aws_cloudtrail_log.aws_cloudtrail_s3 2> /Users/graza/tmp/day.log`
- Time: 1.86s user 2.04s system 9% cpu 40.684 total
  - artifacts discovered: 611, artifacts downloaded: 611, artifacts extracted: 611, rows enriched: 0, rows converted: 0, errors: 0
  - Timing (may overlap):
    - discovery: 34.995955s
    - download:  35.821117s
    - extract:   35.43374s
    - :          0s 

### AWS S3 Sync
**us-east-1**
- Command: `time aws s3 sync s3://aws-cloudtrail-logs-165086849490-3b37f8dd/AWSLogs/o-z3cf4qoe7m/287590803701/CloudTrail/us-east-1/2024/07/31/ /Users/graza/tailpipe_data/cts3/sync_day/ --profile saas_master`
- Time: 1.90s user 0.46s system 12% cpu 18.555 total

**us-east-2**
- Command: `time aws s3 sync s3://aws-cloudtrail-logs-165086849490-3b37f8dd/AWSLogs/o-z3cf4qoe7m/287590803701/CloudTrail/us-east-2/2024/07/31/ /Users/graza/tailpipe_data/cts3/sync_day2/ --profile saas_master`
- Time:  1.92s user 1.25s system 6% cpu 49.938 total

### seqsense/s3sync
**us-east-1**
- Time: 11.702983833s

**us-east-2**
- Time: 32.898680916s

## One Month
- Bucket: `aws-cloudtrail-logs-165086849490-3b37f8dd`
- Prefix: `AWSLogs/o-z3cf4qoe7m/287590803701/CloudTrail/us-east-1/2024/07/` Downloaded Size: `822784KB`
- Prefix: `AWSLogs/o-z3cf4qoe7m/287590803701/CloudTrail/us-east-2/2024/07/` Downloaded Size: `10421392KB` (More size than 1 year of us-east-1, less files)

### Tailpipe

**Concurrency 10 us-east-1**
- Command: `time tailpipe collect --config-path /Users/graza/.tailpipe/ --collection aws_cloudtrail_log.aws_cloudtrail_s3`
- Time: 
  - artifacts discovered: 29706, artifacts downloaded: 29706, artifacts extracted: 29706, rows enriched: 2663187, rows converted: 2663187, errors: 0
  - Timing (may overlap):
    - discovery: 9m41.653063s
    - download:  9m41.033154s
    - extract:   9m40.858699s
    - enrich:    9m40.857975s

**Concurrency 16 us-east-1**
- Command: `time tailpipe collect --config-path /Users/graza/.tailpipe/ --collection aws_cloudtrail_log.aws_cloudtrail_s3`
- Time: 619.34s user 110.62s system 169% cpu 7:10.28 total
  - artifacts discovered: 29706, artifacts downloaded: 29706, artifacts extracted: 29706, rows enriched: 2663187, rows converted: 2663187, errors: 0
  - Timing (may overlap):
    - discovery: 7m1.538743s
    - download:  7m1.081036s
    - extract:   7m0.938817s
    - enrich:    7m0.933889s

**Concurrency 16 us-east-2 NO PROCESSING OF FILES**
- Command: `time tailpipe collect --config-path /Users/graza/.tailpipe/ --collection aws_cloudtrail_log.aws_cloudtrail_s3 2> /Users/graza/tmp/month.log`
- Time: 222.71s user 83.43s system 23% cpu 21:40.38 total
  - artifacts discovered: 20989, artifacts downloaded: 20989, artifacts extracted: 20989, rows enriched: 0, rows converted: 0, errors: 0
  - Timing (may overlap):
    - discovery: 21m38.569117s
    - download:  21m38.728811s
    - extract:   21m38.557009s
    - :          0s

### AWS S3 Sync
**us-east-1**
- Command: `time aws s3 sync s3://aws-cloudtrail-logs-165086849490-3b37f8dd/AWSLogs/o-z3cf4qoe7m/287590803701/CloudTrail/us-east-1/2024/07/ /Users/graza/tailpipe_data/cts3/sync_month/ --profile saas_master`
- Time: 71.05s user 20.06s system 17% cpu 8:55.19 total

**us-east-2**
- Command: `time aws s3 sync s3://aws-cloudtrail-logs-165086849490-3b37f8dd/AWSLogs/o-z3cf4qoe7m/287590803701/CloudTrail/us-east-2/2024/07/ /Users/graza/tailpipe_data/cts3/sync_month2/ --profile saas_master`
- Time: 119.06s user 76.86s system 14% cpu 21:55.13 total

### seqsense/s3sync
**us-east-1**
- Time: 5m35.155627833s

**us-east-2**
- Time: 21m23.015040541s

## One Year
Using 2023 as a complete year:
- Bucket: `aws-cloudtrail-logs-165086849490-3b37f8dd`
- Prefix: `AWSLogs/o-z3cf4qoe7m/287590803701/CloudTrail/us-east-1/2023/` Download Size: `8864888KB`

### Tailpipe
**Concurrency 10 us-east-1**
- Command: `time tailpipe collect --config-path /Users/graza/.tailpipe/ --collection aws_cloudtrail_log.aws_cloudtrail_s3`
- Time: DNR

**Concurrency 16 us-east-1** [Note: This seems to be a glitch have re-run this below twice without the processing and again as per this run and both sub 1hr]
- Command: `time tailpipe collect --config-path /Users/graza/.tailpipe/ --collection aws_cloudtrail_log.aws_cloudtrail_s3`
- Time: 44524.41s user 5407.38s system 156% cpu 8:50:23.82 total
  - artifacts discovered: 294762, artifacts downloaded: 294762, artifacts extracted: 294762, rows enriched: 29580295, rows converted: 29580295, errors: 0
  - Timing (may overlap):
    - discovery: 8h50m14.93161s
    - download:  8h50m17.65025s
    - extract:   8h50m17.719652s
    - enrich:    8h50m17.713187s 

**Concurrency 16 us-east-1 NO PROCESSING OF FILES**
- Command: `time tailpipe collect --config-path /Users/graza/.tailpipe/ --collection aws_cloudtrail_log.aws_cloudtrail_s3`
- Time: 293.57s user 208.95s system 14% cpu 59:10.91 total
  - artifacts discovered: 294762, artifacts downloaded: 294762, artifacts extracted: 294762, rows enriched: 0, rows converted: 0, errors: 0
  - Timing (may overlap):
    - discovery: 59m8.876716s
    - download:  59m8.383367s
    - extract:   59m8.17901s
    - :          0s

**Concurrency 16 us-east-1**
- Command: `time tailpipe collect --config-path /Users/graza/.tailpipe/ --collection aws_cloudtrail_log.aws_cloudtrail_s3`
- Time: 2352.13s user 700.21s system 87% cpu 57:52.13 total
  - artifacts discovered: 294762, artifacts downloaded: 294762, artifacts extracted: 294762, rows enriched: 29580295, rows converted: 29580295, errors: 0
  - Timing (may overlap):
    - discovery: 57m47.621114s
    - download:  57m47.065211s
    - extract:   57m46.915029s
    - enrich:    57m46.908819s

### AWS S3 Sync
**us-east-1**
- Command: `time aws s3 sync s3://aws-cloudtrail-logs-165086849490-3b37f8dd/AWSLogs/o-z3cf4qoe7m/287590803701/CloudTrail/us-east-1/2023/ /Users/graza/tailpipe_data/cts3/sync_year/ --profile saas_master`
- Time: 518.56s user 131.99s system 12% cpu 1:29:20.31 total

### seqsense/s3sync
**us-east-1**
- Time: 57m35.131476333s

## Observations
- `aws s3 sync` does have an option for bucket to bucket sync - potentially avoiding needing large local storage?
- ~~`tailpipe` performance seems to degrade rapidly after a certain point in time~~ This seems to have been a one off, managed to re-run same dataset twice after and obtained normal results.
- We do get slow downs when we enable paging on large data sets where we're serializing 100's of MB for paging data, this is being reviewed and shouldn't be an issue going forward.

## App for seqsense/s3sync testing

The below was built as a binary and ran for the timings.

```go
package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/seqsense/s3sync"
	"time"
)

func main() {
	sess, _ := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: aws.String("us-east-2"),
		},
		Profile: "saas_master",
	})
	syncMan := s3sync.New(sess)

	startDay := time.Now()
	_ = syncMan.Sync("s3://aws-cloudtrail-logs-165086849490-3b37f8dd/AWSLogs/o-z3cf4qoe7m/287590803701/CloudTrail/us-east-1/2024/07/31/", "/Users/graza/tailpipe_data/cts3/seq_day/")
	endDay := time.Now()
	dayDuration := endDay.Sub(startDay)

	startMonth := time.Now()
	_ = syncMan.Sync("s3://aws-cloudtrail-logs-165086849490-3b37f8dd/AWSLogs/o-z3cf4qoe7m/287590803701/CloudTrail/us-east-1/2024/07/", "/Users/graza/tailpipe_data/cts3/seq_month/")
	endMonth := time.Now()
	monthDuration := endMonth.Sub(startMonth)

  startYear := time.Now()
	_ = syncMan.Sync("s3://aws-cloudtrail-logs-165086849490-3b37f8dd/AWSLogs/o-z3cf4qoe7m/287590803701/CloudTrail/us-east-1/2023/", "/Users/graza/tailpipe_data/cts3/seq_year/")
	endYear := time.Now()
	yearDuration := endYear.Sub(startYear)

	fmt.Printf("Day: %v\n", dayDuration)
	fmt.Printf("Month: %v\n", monthDuration)
  fmt.Printf("Year: %v\n", yearDuration)
}
```

## tl;dr

Downloading of files from tailpipe seems to be on par with `aws s3 cli` & `seqsense/s3sync` when given the same parameters for concurrency.
