package artifact_source

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
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/rate_limiter"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

//TODO #delta is timestamp reliable - do logs always come in order? is it should we/are we using ingestion time? https://github.com/turbot/tailpipe-plugin-sdk/issues/5

const (
	AWSCloudwatchSourceIdentifier = "aws_cloudwatch"
)

// TODO #config TMP https://github.com/turbot/tailpipe-plugin-sdk/issues/3
const BaseTmpDir = "/tmp/tailpipe"

func init() {
	// register source
	row_source.Factory.RegisterRowSources(NewAwsCloudWatchSource)
}

// AwsCloudWatchSource is a [ArtifactSource] implementation that reads logs from AWS CloudWatch
// and writes them to a temp JSON file
type AwsCloudWatchSource struct {
	ArtifactSourceBase[*artifact_source_config.AwsCloudWatchSourceConfig]

	client  *cloudwatchlogs.Client
	limiter *rate_limiter.APILimiter
}

func NewAwsCloudWatchSource() row_source.RowSource {
	return &AwsCloudWatchSource{}
}

func (s *AwsCloudWatchSource) Init(ctx context.Context, configData *parse.Data, opts ...row_source.RowSourceOption) error {
	// call base to parse config and apply options
	if err := s.ArtifactSourceBase.Init(ctx, configData, opts...); err != nil {
		return err
	}
	// NOTE: add the cloudwatch mapper to ensure rows are in correct format
	s.AddMappers(artifact_mapper.NewCloudwatchMapper())

	s.TmpDir = path.Join(BaseTmpDir, fmt.Sprintf("cloudwatch-%s", s.Config.LogGroupName))

	// initialize client
	client, err := s.getClient(ctx)
	if err != nil {
		return err
	}
	s.client = client

	// TODO NEEDED? https://github.com/turbot/tailpipe-plugin-sdk/issues/6
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

func (s *AwsCloudWatchSource) GetConfigSchema() parse.Config {
	return &artifact_source_config.AwsCloudWatchSourceConfig{}
}

// Close deletes the temp directory and all files
func (s *AwsCloudWatchSource) Close() error {
	// delete the temp dir and all files
	return os.RemoveAll(s.TmpDir)
}

// DiscoverArtifacts gets the log streams for the configured log group and log stream prefix,
// within the configured time range, and respecting the time range in the collection state data
func (s *AwsCloudWatchSource) DiscoverArtifacts(ctx context.Context) error {
	collectionState, _ := s.CollectionState.(*collection_state.AwsCloudwatchState)

	input := &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName: &s.Config.LogGroupName,
		// // set prefix (this may be nil)
		LogStreamNamePrefix: s.Config.LogStreamPrefix,
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
			startTime, endTime := s.getTimeRange(streamName, collectionState)
			// does this stream have entries within this time range
			if !logStreamNameWithinTimeRange(logStream, startTime, endTime) {
				inactiveCount++
				continue
			}
			activeCount++

			// populate enrichment fields the the source is aware of
			// - in this case the source type and name
			// TODO #enrich check these https://github.com/turbot/tailpipe-plugin-sdk/issues/7
			sourceEnrichmentFields := &enrichment.CommonFields{
				TpSourceType: "cloudwatch",
				TpSourceName: streamName,
			}

			// TODO #error handle rate limiting errors
			info := types.NewArtifactInfo(streamName, types.WithEnrichmentFields(sourceEnrichmentFields))
			// handle the artifact discovery - trigger a download and notify observers
			if err := s.OnArtifactDiscovered(ctx, info); err != nil {
				// TODO #error - should we return an error here or gather all errors?
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

// DownloadArtifact gets the log events for the specified log stream,
// respecting the time range in the config and collection state data
func (s *AwsCloudWatchSource) DownloadArtifact(ctx context.Context, info *types.ArtifactInfo) error {
	// get the collection state data
	collectionState, _ := s.CollectionState.(*collection_state.AwsCloudwatchState)

	// get the time range for the log stream
	startTime, endTime := s.getTimeRange(info.Name, collectionState)
	// if start time is after the end time, return
	if startTime >= endTime {
		slog.Info("DownloadArtifact - log stream already downloaded", "log stream", info.Name)
		return nil
	}

	input := &cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  &s.Config.LogGroupName,
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

	// keep track of the max time for the collection state data
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
	downloadInfo := types.NewArtifactInfo(localFilePath)

	// update collection state data for this log stream
	collectionState.Upsert(info.Name, maxTime)

	return s.OnArtifactDownloaded(ctx, downloadInfo)
}

// use the collection state data (if present) and the configured time range to determine the start and end time
func (s *AwsCloudWatchSource) getTimeRange(logStream string, collectionState *collection_state.AwsCloudwatchState) (int64, int64) {
	startTime := s.Config.StartTime.UnixMilli()
	endTime := s.Config.EndTime.UnixMilli()

	if collectionState != nil {
		// set start time from collection state data if present
		if prevTimestamp, ok := collectionState.Timestamps[logStream]; ok {
			startTime = prevTimestamp + 1
		}
	}
	return startTime, endTime
}

func (s *AwsCloudWatchSource) getClient(ctx context.Context) (*cloudwatchlogs.Client, error) {
	var opts []func(*config.LoadOptions) error
	// TODO handle all credential types https://github.com/turbot/tailpipe-plugin-sdk/issues/8
	// add credentials if provided
	if s.Config.AccessKey != "" && s.Config.SecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(s.Config.AccessKey, s.Config.SecretKey, s.Config.SessionToken)))
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
