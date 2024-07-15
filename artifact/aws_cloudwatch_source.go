package artifact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cloudwatch_types "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type AwsCloudWatchSourceConfig struct {
	AccessKey    string
	SecretKey    string
	SessionToken string

	// the log group to collect
	// assume a source will be used to fetch a single log group?
	LogGroupName string
	// the log stream(s) to collect
	// or should this be based on what discover artifacts returns
	//LogStreams []string
}

type AwsCloudWatchSource struct {
	SourceBase

	Config *AwsCloudWatchSourceConfig
	TmpDir string
	client *cloudwatchlogs.Client
}

func NewAwsCloudWatchSource(config *AwsCloudWatchSourceConfig) (*AwsCloudWatchSource, error) {
	s := &AwsCloudWatchSource{
		Config: config,
	}
	s.TmpDir = path.Join(os.TempDir(), "tailpipe", "cloudwatch")

	if err := s.ValidateConfig(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// initialize client
	client, err := s.getClient(context.Background())
	if err != nil {
		return nil, err
	}
	s.client = client

	return s, nil
}

func (s *AwsCloudWatchSource) Identifier() string {
	return "aws_cloudwatch"
}

func (s *AwsCloudWatchSource) Close() error {
	// delete the temp dir and all files
	return os.RemoveAll(s.TmpDir)
}

func (s *AwsCloudWatchSource) ValidateConfig() error {
	return nil
}

func (s *AwsCloudWatchSource) DiscoverArtifacts(ctx context.Context, req *proto.CollectRequest) error {
	slog.Debug("AwsCloudWatchSource ")

	input := &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName: &s.Config.LogGroupName,
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
			if err := s.OnArtifactDiscovered(req, info); err != nil {
				// TODO #err - should we return an error here or gather all errors?
				return fmt.Errorf("failed to notify observers of discovered artifact, %w", err)
			}
		}
	}
	return nil
}

func (s *AwsCloudWatchSource) DownloadArtifact(ctx context.Context, req *proto.CollectRequest, info *types.ArtifactInfo) error {

	// Define the query string to filter logs from the specified log stream
	queryString := fmt.Sprintf("fields @timestamp, @message | filter @logStream == '%s' | sort @timestamp desc", info.Name)
	// TODO hacked to fetch last 24 hours
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
	for {
		time.Sleep(5 * time.Second)

		getQueryResultsInput := &cloudwatchlogs.GetQueryResultsInput{
			QueryId: &queryID,
		}

		getQueryResultsOutput, err := s.client.GetQueryResults(context.TODO(), getQueryResultsInput)
		if err != nil {
			return fmt.Errorf("failed to get query results, %w", err)
		}

		if getQueryResultsOutput.Status == cloudwatch_types.QueryStatusComplete || getQueryResultsOutput.Status == cloudwatch_types.QueryStatusFailed || getQueryResultsOutput.Status == cloudwatch_types.QueryStatusCancelled {

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
		}
	}

	// notify observers of the discovered artifact
	downloadInfo := &types.ArtifactInfo{Name: localFilePath, OriginalName: info.Name}

	return s.OnArtifactDownloaded(req, downloadInfo)
}

//func (s *AwsCloudWatchSource) DownloadArtifact(ctx context.Context, req *proto.CollectRequest, info *types.ArtifactInfo) error {
//	// TODO we need a way of specifying start/end times - an option to DownloadArtifact - or propertied on artifact info?
//	input := &cloudwatchlogs.GetLogEventsInput{
//		LogGroupName:  &s.Config.LogGroupName,
//		LogStreamName: &info.Name,
//		//StartTime:     &startTime,
//		//EndTime:       &endTime,
//		//StartFromHead: true,
//	}
//
//	// TODO IS THIS OK/CORRECT
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
//
//	paginator := cloudwatchlogs.NewGetLogEventsPaginator(s.client, input)
//	for paginator.HasMorePages() {
//
//		// retry the paginator to allow for rate limit errors
//		// TODO should we rate limit these page calls?
//		retry.Do(context.Background(), retry.NewConstant(5*time.Second), func(ctx context.Context) error {
//
//			output, err := paginator.NextPage(ctx)
//			if err != nil {
//				// TODO handle rate limiting errors nicer
//				// is itr a ratelimit.QuotaExceededError?
//				if IsRateLimitError(err){
//					return retry.RetryableError(fmt.Errorf("rate limit exceeded, %w", err))
//				}
//				return fmt.Errorf("failed to get log events, %w", err)
//			}
//
//			for _, event := range output.Events {
//				err := enc.Encode(event)
//				if err != nil {
//					return fmt.Errorf("failed to write event to file, %w", err)
//				}
//			}
//			return nil
//		})
//
//		// todo hack
//		time.Sleep(100*time.Millisecond)
//	}
//
//
//	// notify observers of the discovered artifact
//	downloadInfo := &types.ArtifactInfo{Name: localFilePath, OriginalName: info.Name}
//
//	return s.OnArtifactDownloaded(req, downloadInfo)
//}

func IsRateLimitError(err error) bool {
	return errors.Is(err, ratelimit.QuotaExceededError{}) ||
		strings.Contains(err.Error(), "Rate exceeded")
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
