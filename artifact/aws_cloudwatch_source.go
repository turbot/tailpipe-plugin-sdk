package artifact

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cloudwatch_types "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/sethvargo/go-retry"
	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/rate_limiter"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"os"
	"path"
	"path/filepath"
	"time"
)

type AwsCloudWatchSourceConfig struct {
	AccessKey    string
	SecretKey    string
	SessionToken string

	// the log group to collect
	// assume a source will be used to fetch a single log group?
	LogGroupName   string
	LogGroupPrefix *string

	// the log stream(s) to collect
	// or should this be based on what discover artifacts returns
	//LogStreams []string
}

type AwsCloudWatchSource struct {
	SourceBase

	Config  *AwsCloudWatchSourceConfig
	TmpDir  string
	client  *cloudwatchlogs.Client
	limiter *rate_limiter.APILimiter
}

func NewAwsCloudWatchSource(config *AwsCloudWatchSourceConfig) (*AwsCloudWatchSource, error) {
	s := &AwsCloudWatchSource{
		Config: config,
	}
	// TODO configure the temp dir location
	// TODO ensure it is cleaned up
	p, _ := filepath.Abs(path.Join("." /*os.TempDir()*/, "tailpipe", "cloudwatch"))
	s.TmpDir = p

	if err := s.ValidateConfig(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// initialize client
	client, err := s.getClient(context.Background())
	if err != nil {
		return nil, err
	}
	s.client = client

	// TODO NEEDED?
	s.limiter = rate_limiter.NewAPILimiter(&rate_limiter.Definition{
		Name:       "cloudwatch_limiter",
		FillRate:   5,
		BucketSize: 5,
	})

	return s, nil
}

func (s *AwsCloudWatchSource) Identifier() string {
	return AWSCloudwatchLoaderIdentifier
}

// Mapper returns a function that creates a new Mapper required by this source
// CloudwatchMapper knows how to extract the row and metadata fields from the JSON saved by the AwsCloudWatchSource
func (s *AwsCloudWatchSource) Mapper() func() Mapper {
	return NewCloudwatchMapper
}

func (s *AwsCloudWatchSource) Close() error {
	// delete the temp dir and all files
	return os.RemoveAll(s.TmpDir)
}

func (s *AwsCloudWatchSource) ValidateConfig() error {
	return nil
}

func (s *AwsCloudWatchSource) DiscoverArtifacts(ctx context.Context, req *proto.CollectRequest) error {
	input := &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName: &s.Config.LogGroupName,
		// this may be nil
		LogStreamNamePrefix: s.Config.LogGroupPrefix,
	}

	paginator := cloudwatchlogs.NewDescribeLogStreamsPaginator(s.client, input)

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			// Handle error
			return fmt.Errorf("failed to get log streams, %w", err)
		}

		for _, logStream := range output.LogStreams {
			streamName := typehelpers.SafeString(logStream.LogStreamName)
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
			if err := s.OnArtifactDiscovered(ctx, req, info); err != nil {
				// TODO #err - should we return an error here or gather all errors?
				return fmt.Errorf("failed to notify observers of discovered artifact, %w", err)
			}
		}
	}
	return nil
}

func (s *AwsCloudWatchSource) DownloadArtifactsWithFilter(ctx context.Context, req *proto.CollectRequest, info *types.ArtifactInfo) error {
	// Define the query string to filter logs from the specified log stream
	queryString := fmt.Sprintf("fields @timestamp as Timestamp, @message as Message, @ingestionTime as IngestionTime | filter @logStream == '%s' | sort @timestamp desc", info.Name)

	// TODO where do these come from
	startTime := time.Now().Add(-24 * time.Hour).Unix()
	endTime := time.Now().Unix()

	startQueryInput := &cloudwatchlogs.StartQueryInput{
		LogGroupName: &s.Config.LogGroupName,
		QueryString:  &queryString,
		StartTime:    &startTime,
		EndTime:      &endTime,
	}

	startQueryOutput, err := s.client.StartQuery(context.TODO(), startQueryInput)
	if err != nil {
		return fmt.Errorf("failed to start query, %w", err)
	}

	queryID := *startQueryOutput.QueryId

	// TODO IS THIS OK/CORRECT
	// copy the object data to a temp file
	localFilePath := path.Join(s.TmpDir, info.Name)
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
	enc := json.NewEncoder(outFile)

	// Poll for query results
	timeout := 5 * time.Minute
	err = retry.Do(context.Background(), retry.WithMaxDuration(timeout, retry.NewConstant(50*time.Millisecond)), func(ctx context.Context) error {
		// apply rate limiter
		// TODO necessary - we can just control backoff?
		if err := s.limiter.Wait(ctx); err != nil {
			return fmt.Errorf("error acquiring rate limiter: %w", err)
		}

		getQueryResultsInput := &cloudwatchlogs.GetQueryResultsInput{
			QueryId: &queryID,
		}

		getQueryResultsOutput, err := s.client.GetQueryResults(ctx, getQueryResultsInput)
		if err != nil {
			return fmt.Errorf("failed to get query results, %w", err)
		}

		isComplete := getQueryResultsOutput.Status == cloudwatch_types.QueryStatusComplete || getQueryResultsOutput.Status == cloudwatch_types.QueryStatusFailed || getQueryResultsOutput.Status == cloudwatch_types.QueryStatusCancelled
		if !isComplete {
			return retry.RetryableError(fmt.Errorf("query not complete, %w", err))
		}

		for _, result := range getQueryResultsOutput.Results {
			row := make(map[string]string)
			for _, field := range result {
				row[*field.Field] = *field.Value
			}
			err := enc.Encode(row)
			if err != nil {
				return fmt.Errorf("failed to write event to file, %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to get query results, %w", err)
	}

	// notify observers of the discovered artifact
	downloadInfo := &types.ArtifactInfo{Name: localFilePath, OriginalName: info.Name}

	return s.OnArtifactDownloaded(ctx, req, downloadInfo)
}

func (s *AwsCloudWatchSource) DownloadArtifact(ctx context.Context, req *proto.CollectRequest, info *types.ArtifactInfo) error {
	// TODO confiug should specify wild cards for log streams
	// TODO we need a way of specifying start/end times - an option to DownloadArtifact - or propertied on artifact info?

	startTime := time.Now().Add(-24 * time.Hour).UnixMilli()
	endTime := time.Now().Add(-1 * time.Hour).UnixMilli()

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
	enc := json.NewEncoder(outFile)

	paginator := cloudwatchlogs.NewGetLogEventsPaginator(s.client, input)
	//for paginator.HasMorePages() {
	//
	//	// retry the paginator to allow for rate limit errors
	//	// TODO should we rate limit these page calls?
	//	retry.Do(context.Background(), retry.NewConstant(500*time.Millisecond), func(ctx context.Context) error {
	//
	//		// apply rate limiter
	//		if err := s.limiter.Wait(ctx); err != nil {
	//			return fmt.Errorf("error acquiring rate limiter: %w", err)
	//		}
	//
	//		output, err := paginator.NextPage(ctx)
	//		if err != nil {
	//			// TODO handle rate limiting errors nicer
	//			// is itr a ratelimit.QuotaExceededError?
	//			if IsRateLimitError(err) {
	//				return retry.RetryableError(fmt.Errorf("rate limit exceeded, %w", err))
	//			}
	//			return fmt.Errorf("failed to get log events, %w", err)
	//		}
	//
	//		for _, event := range output.Events {
	//			err := enc.Encode(event)
	//			if err != nil {
	//				return fmt.Errorf("failed to write event to file, %w", err)
	//			}
	//		}
	//		return nil
	//	})
	//
	//}

	var previousToken *string
	for paginator.HasMorePages() {
		// retry the paginator to allow for rate limit errors
		// TODO should we rate limit these page calls?

		// apply rate limiter
		if err := s.limiter.Wait(ctx); err != nil {
			return fmt.Errorf("error acquiring rate limiter: %w", err)
		}
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get log events, %w", err)
		}

		for _, event := range output.Events {
			if err := enc.Encode(event); err != nil {
				return fmt.Errorf("failed to write event to file, %w", err)
			}
		}

		// Break the loop if the NextToken hasn't changed, indicating all data has been fetched
		if previousToken != nil && output.NextForwardToken != nil && *previousToken == *output.NextForwardToken {
			break
		}
		previousToken = output.NextForwardToken
	}

	// notify observers of the discovered artifact
	downloadInfo := &types.ArtifactInfo{Name: localFilePath, OriginalName: info.Name}

	return s.OnArtifactDownloaded(ctx, req, downloadInfo)
}

func (s *AwsCloudWatchSource) getClient(ctx context.Context) (*cloudwatchlogs.Client, error) {
	var opts []func(*config.LoadOptions) error
	// TODO handle all credential types
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
