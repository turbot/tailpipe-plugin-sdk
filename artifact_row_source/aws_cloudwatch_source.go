package artifact_row_source

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cloudwatch_types "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_mapper"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"github.com/turbot/tailpipe-plugin-sdk/rate_limiter"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

const (
	AWSCloudwatchSourceIdentifier = "aws_cloudwatch"
)

func init() {
	// register source
	Factory.RegisterArtifactSources(NewAwsCloudWatchSource)
}

// AwsCloudWatchSource is a [Source] implementation that reads logs from AWS CloudWatch
// and writes them to a temp JSON file
type AwsCloudWatchSource struct {
	Base[AwsCloudWatchSourceConfig]

	client  *cloudwatchlogs.Client
	limiter *rate_limiter.APILimiter
}

func NewAwsCloudWatchSource() Source {
	return &AwsCloudWatchSource{}
}

func (s *AwsCloudWatchSource) Init(ctx context.Context, configData *hcl.Data) error {
	// parse the config
	var c, _, err = hcl.ParseConfig[AwsCloudWatchSourceConfig](configData)
	if err != nil {
		slog.Error("AwsS3BucketSource Init - error parsing config", "error", err)
		return err
	}

	s.config = c

	s.TmpDir = path.Join(BaseTmpDir, fmt.Sprintf("cloudwatch-%s", c.LogGroupName))

	if err := s.ValidateConfig(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// initialize client
	client, err := s.getClient(ctx)
	if err != nil {
		return err
	}
	s.client = client

	// TODO NEEDED?
	s.limiter = rate_limiter.NewAPILimiter(&rate_limiter.Definition{
		Name:       "cloudwatch_limiter",
		FillRate:   5,
		BucketSize: 5,
	})

	return nil
}

func (s *AwsCloudWatchSource) Identifier() string {
	return AWSCloudwatchSourceIdentifier
}

// Mapper returns a function that creates a new [Mapper] required by this source
// [CloudwatchMapper] knows how to extract the row and metadata fields from the JSON that we save
func (s *AwsCloudWatchSource) Mapper() func() artifact_mapper.Mapper {
	return artifact_mapper.NewCloudwatchMapper
}

// Close deletes the temp directory and all files
func (s *AwsCloudWatchSource) Close() error {
	// delete the temp dir and all files
	return os.RemoveAll(s.TmpDir)
}

// ValidateConfig checks the config for required fields
func (s *AwsCloudWatchSource) ValidateConfig() error {
	// #TODO #config - validate the config
	return nil
}

// DiscoverArtifacts gets the log streams for the configured log group and log stream prefix,
// within the configured time range, and respecting the time range in the paging data
func (s *AwsCloudWatchSource) DiscoverArtifacts(ctx context.Context) error {
	pagingData, _ := s.PagingData.(*paging.Cloudwatch)

	input := &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName: &s.config.LogGroupName,
		// // set prefix (this may be nil)
		LogStreamNamePrefix: s.config.LogStreamPrefix,
	}

	paginator := cloudwatchlogs.NewDescribeLogStreamsPaginator(s.client, input)

	var activeCount, inactiveCount int
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			// Handle error
			return fmt.Errorf("failed to get log streams, %w", err)
		}

		for _, logStream := range output.LogStreams {
			streamName := typehelpers.SafeString(logStream.LogStreamName)

			// get the time range of interest for this stream,
			startTime, endTime := s.getTimeRange(ctx, streamName, pagingData)
			// does this stream have entries within this time range
			if !logStreamNameWithinTimeRange(logStream, startTime, endTime) {
				inactiveCount++
				continue
			}
			activeCount++

			// populate enrichment fields the the source is aware of
			// - in this case the source type and name
			// TODO #enrich check these
			sourceEnrichmentFields := &enrichment.CommonFields{
				TpSourceType: "cloudwatch",
				TpSourceName: streamName,
			}

			// TODO handle rate limiting errors
			info := &types.ArtifactInfo{Name: streamName, EnrichmentFields: sourceEnrichmentFields}
			// notify observers of the discovered artifact
			if err := s.OnArtifactDiscovered(ctx, info); err != nil {
				// TODO #err - should we return an error here or gather all errors?
				return fmt.Errorf("failed to notify observers of discovered artifact, %w", err)
			}
		}
	}
	slog.Info("DiscoverArtifacts - log streams discovered", "active", activeCount, "inactive", inactiveCount)
	return nil
}

func logStreamNameWithinTimeRange(logStream cloudwatch_types.LogStream, startTime, endTime int64) bool {
	if logStream.LastIngestionTime == nil || logStream.FirstEventTimestamp == nil {
		return false
	}
	return *logStream.LastIngestionTime > startTime && *logStream.FirstEventTimestamp < endTime
}

//func (s *AwsCloudWatchSource) DownloadArtifactsWithFilter(ctx context.Context, info *types.ArtifactInfo) error {
//	// Define the query string to filter logs from the specified log stream
//	queryString := fmt.Sprintf("fields @timestamp as Timestamp, @message as Message, @ingestionTime as IngestionTime | filter @logStream == '%s' | sort @timestamp desc", info.Name)
//
//	startTime, endTime := s.getTimeRange(ctx, info)
//	// if start time is after the end time, return
//	if startTime >= endTime {
//		slog.Info("DownloadArtifact - log stream already downloaded", "log stream", info.Name)
//		return nil
//	}
//
//	startQueryInput := &cloudwatchlogs.StartQueryInput{
//		LogGroupName: &s.config.LogGroupName,
//		QueryString:  &queryString,
//		StartTime:    &startTime,
//		EndTime:      &endTime,
//	}
//
//	startQueryOutput, err := s.client.StartQuery(ctx, startQueryInput)
//	if err != nil {
//		return fmt.Errorf("failed to start query, %w", err)
//	}
//
//	queryID := *startQueryOutput.QueryId
//
//	// copy the object data to a temp file
//	localFilePath := path.Join(s.TmpDir, info.Name)
//	// ensure the directory exists of the file to write to
//	if err := os.MkdirAll(filepath.Dir(localFilePath), 0755); err != nil {
//		return fmt.Errorf("failed to create directory for file, %w", err)
//	}
//
//	// Create a local file to write the data to
//	outFile, err := os.Create(localFilePath)
//	if err != nil {
//		return fmt.Errorf("failed to create file, %w", err)
//	}
//	defer outFile.Close()
//	enc := json.NewEncoder(outFile)
//
//	// Poll for query results
//	timeout := 5 * time.Minute
//	err = retry.Do(ctx, retry.WithMaxDuration(timeout, retry.NewConstant(50*time.Millisecond)), func(ctx context.Context) error {
//		// apply rate limiter
//		// TODO necessary - we can just control backoff?
//		if err := s.limiter.Wait(ctx); err != nil {
//			return fmt.Errorf("error acquiring rate limiter: %w", err)
//		}
//
//		getQueryResultsInput := &cloudwatchlogs.GetQueryResultsInput{
//			QueryId: &queryID,
//		}
//
//		getQueryResultsOutput, err := s.client.GetQueryResults(ctx, getQueryResultsInput)
//		if err != nil {
//			return fmt.Errorf("failed to get query results, %w", err)
//		}
//
//		isComplete := getQueryResultsOutput.Status == cloudwatch_types.QueryStatusComplete || getQueryResultsOutput.Status == cloudwatch_types.QueryStatusFailed || getQueryResultsOutput.Status == cloudwatch_types.QueryStatusCancelled
//		if !isComplete {
//			return retry.RetryableError(fmt.Errorf("query not complete, %w", err))
//		}
//
//		for _, result := range getQueryResultsOutput.Results {
//			row := make(map[string]string)
//			for _, field := range result {
//				row[*field.Field] = *field.Value
//			}
//			err := enc.Encode(row)
//			if err != nil {
//				return fmt.Errorf("failed to write event to file, %w", err)
//			}
//		}
//		return nil
//	})
//	if err != nil {
//		return fmt.Errorf("failed to get query results, %w", err)
//	}
//
//	// notify observers of the discovered artifact
//	downloadInfo := &types.ArtifactInfo{Name: localFilePath, OriginalName: info.Name}
//
//	return s.OnArtifactDownloaded(ctx, downloadInfo, nil)
//}

// DownloadArtifact gets the log events for the specified log stream,
// respecting the time range in the config and paging data
func (s *AwsCloudWatchSource) DownloadArtifact(ctx context.Context, info *types.ArtifactInfo) error {
	// get the paging data
	pagingData, _ := s.PagingData.(*paging.Cloudwatch)

	// get the time range for the log stream
	startTime, endTime := s.getTimeRange(ctx, info.Name, pagingData)
	// if start time is after the end time, return
	if startTime >= endTime {
		slog.Info("DownloadArtifact - log stream already downloaded", "log stream", info.Name)
		return nil
	}

	input := &cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  &s.config.LogGroupName,
		LogStreamName: &info.Name,
		StartTime:     &startTime,
		EndTime:       &endTime,
	}

	// copy the object data to a temp file
	localFilePath := path.Join(s.TmpDir, fmt.Sprintf("%s.json", info.Name))
	// ensure the directory exists of the file to write to
	if err := os.MkdirAll(filepath.Dir(localFilePath), 0755); err != nil {
		return fmt.Errorf("failed to create directory for file, %w", err)
	}

	// Create a local file to write the data to
	outFile, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("failed to create file, %w", err)
	}
	defer outFile.Close()
	// create an encoder to write the events to the file
	enc := json.NewEncoder(outFile)

	// keep track of the max time for the paging data
	var maxTime int64
	// event count
	var count int

	paginator := cloudwatchlogs.NewGetLogEventsPaginator(s.client, input)
	var previousToken *string
	for paginator.HasMorePages() {
		// apply rate limiter
		//if err := s.limiter.Wait(ctx); err != nil {
		//	return fmt.Errorf("error acquiring rate limiter: %w", err)
		//}
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get log events, %w", err)
		}

		for _, event := range output.Events {
			count++
			ts := *event.Timestamp
			slog.Debug("DownloadArtifact - writing event to file", "artifact", info.Name, "ts", *event.Timestamp, "maxTime", maxTime)
			// update the max time
			if ts > maxTime {
				maxTime = *event.Timestamp
			}
			// write the event to the file
			if err := enc.Encode(event); err != nil {
				return fmt.Errorf("failed to write event to file, %w", err)
			}
		}

		// Break the loop if the NextToken hasn't changed, indicating all data has been fetched
		if previousToken != nil && output.NextForwardToken != nil && *previousToken == *output.NextForwardToken {
			slog.Debug("DownloadArtifact - NextForwardToken is same as previous NextForwardToken - all data fetched", "log stream", info.Name)
			break
		}
		previousToken = output.NextForwardToken
	}

	// if no events were found, delete the file and return
	if count == 0 {
		fileErr := os.Remove(localFilePath)
		if fileErr != nil {
			slog.Warn("DownloadArtifact - no events found, failed to delete file", "artifact", info.Name, "file", localFilePath, "error", fileErr)
			return fmt.Errorf("no events found, failed to delete file, %w", fileErr)
		}
		return nil
	}

	// notify observers of the discovered artifact
	downloadInfo := &types.ArtifactInfo{Name: localFilePath, OriginalName: info.Name}

	// build paging data
	pagingData = paging.NewCloudwatch()
	pagingData.Add(info.Name, maxTime)

	return s.OnArtifactDownloaded(ctx, downloadInfo, pagingData)
}

// use the paging data (if present) and the configured time range to determine the start and end time
func (s *AwsCloudWatchSource) getTimeRange(ctx context.Context, logStream string, paging *paging.Cloudwatch) (int64, int64) {
	startTime := s.config.StartTime.UnixMilli()
	endTime := s.config.EndTime.UnixMilli()

	if paging != nil {
		// set start time from paging data if present
		if prevTimestamp, ok := paging.Timestamps[logStream]; ok {
			startTime = prevTimestamp + 1
		}
	}
	return startTime, endTime
}

func (s *AwsCloudWatchSource) getClient(ctx context.Context) (*cloudwatchlogs.Client, error) {
	var opts []func(*config.LoadOptions) error
	// TODO handle all credential types
	// add credentials if provided
	if s.config.AccessKey != "" && s.config.SecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(s.config.AccessKey, s.config.SecretKey, s.config.SessionToken)))
	}
	// TODO do we need to specify a region?
	// add with region
	opts = append(opts, config.WithRegion("us-east-1"))

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config, %w", err)
	}

	client := cloudwatchlogs.NewFromConfig(cfg)
	return client, nil
}
