package artifact_source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

const (
	AwsS3BucketSourceIdentifier = "aws_s3_bucket"
	defaultBucketRegion         = "us-east-1"
)

func init() {
	// register source
	row_source.Factory.RegisterRowSources(NewAwsS3BucketSource)
}

// AwsS3BucketSource is a [ArtifactSource] implementation that reads artifacts from an S3 bucket
type AwsS3BucketSource struct {
	ArtifactSourceImpl[*artifact_source_config.AwsS3BucketSourceConfig]

	Extensions types.ExtensionLookup
	client     *s3.Client
}

func NewAwsS3BucketSource() row_source.RowSource {
	return &AwsS3BucketSource{}
}

func (s *AwsS3BucketSource) Init(ctx context.Context, configData *types.ConfigData, opts ...row_source.RowSourceOption) error {
	slog.Info("Initializing AwsS3BucketSource")

	// set the collection state func to the S3 specific collection state
	s.NewCollectionStateFunc = collection_state.NewAwsS3CollectionState

	// call base to parse config and apply options
	if err := s.ArtifactSourceImpl.Init(ctx, configData, opts...); err != nil {
		return err
	}

	s.Extensions = types.NewExtensionLookup(s.Config.Extensions)
	s.TmpDir = path.Join(BaseTmpDir, fmt.Sprintf("s3-%s", s.Config.Bucket))

	if s.Config.Region == nil {
		slog.Info("No region set, using default", "region", defaultBucketRegion)
		s.Config.Region = utils.ToStringPointer(defaultBucketRegion)
	}
	// initialize client
	client, err := s.getClient(ctx)
	if err != nil {
		return err
	}
	s.client = client

	slog.Info("Initialized AwsS3BucketSource", "bucket", s.Config.Bucket, "prefix", s.Config.Prefix, "extensions", s.Extensions)

	return err
}

func (s *AwsS3BucketSource) Identifier() string {
	return AwsS3BucketSourceIdentifier
}

func (s *AwsS3BucketSource) GetConfigSchema() parse.Config {
	return &artifact_source_config.AwsS3BucketSourceConfig{}
}

func (s *AwsS3BucketSource) Close() error {
	// delete the temp dir and all files
	return os.RemoveAll(s.TmpDir)
}

func (s *AwsS3BucketSource) ValidateConfig() error {
	if s.Config.Bucket == "" {
		return errors.New("bucket is required")
	}

	// Check format of extensions
	var invalidExtensions []string
	for _, e := range s.Config.Extensions {
		if len(e) == 0 {
			invalidExtensions = append(invalidExtensions, "<empty>")
		} else if e[0] != '.' {
			invalidExtensions = append(invalidExtensions, e)
		}
	}
	if len(invalidExtensions) > 0 {
		return fmt.Errorf("invalid extensions: %s", strings.Join(invalidExtensions, ","))
	}

	return nil
}

func (s *AwsS3BucketSource) DiscoverArtifacts(ctx context.Context) error {
	// cast the collection state to the correct type
	collectionState := s.CollectionState.(*collection_state.AwsS3CollectionState)
	// verify this is initialized (i.e. the regex has been created)
	if collectionState == nil || !collectionState.Initialized() {
		return errors.New("collection state not initialized")
	}

	startAfterKey := s.Config.StartAfterKey
	if collectionState.UseStartAfterKey {
		startAfterKey = collectionState.StartAfterKey
	}

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:     &s.Config.Bucket,
		Prefix:     &s.Config.Prefix,
		StartAfter: startAfterKey,
	})

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get page of S3 objects, %w", err)
		}
		for _, object := range output.Contents {
			path := *object.Key

			// check the extension
			if s.Extensions.IsValid(path) {
				// populate enrichment fields the the source is aware of
				// - in this case the source location
				sourceEnrichmentFields := &enrichment.CommonFields{
					TpSourceType:     "aws_s3_bucket",
					TpSourceName:     s.Config.Bucket,
					TpSourceLocation: &path,
				}

				info := types.NewArtifactInfo(path, types.WithEnrichmentFields(sourceEnrichmentFields))

				// extract properties based on the filename
				extractedProperties, err := collectionState.ParseFilename(path)
				if err != nil {
					//	TODO #error #paging what??? download anyway???
					continue
				}
				info.SetPathProperties(extractedProperties)

				if !collectionState.ShouldCollect(info) {
					continue
				}

				// notify observers of the discovered artifact
				if err := s.OnArtifactDiscovered(ctx, info); err != nil {
					// TODO #error should we continue or fail?
					return fmt.Errorf("failed to notify observers of discovered artifact, %w", err)
				}
			}
		}
	}

	return nil
}

func (s *AwsS3BucketSource) DownloadArtifact(ctx context.Context, info *types.ArtifactInfo) error {
	collectionState := s.CollectionState.(*collection_state.AwsS3CollectionState)

	// Get the object from S3
	getObjectOutput, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.Config.Bucket,
		Key:    &info.Name,
	})
	if err != nil {
		return fmt.Errorf("failed to download artifact, %w", err)
	}
	defer getObjectOutput.Body.Close()

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

	// Write the data to the local file
	_, err = io.Copy(outFile, getObjectOutput.Body)
	if err != nil {
		return fmt.Errorf("failed to write data to file, %w", err)
	}

	// notify observers of the discovered artifact
	downloadInfo := &types.ArtifactInfo{Name: localFilePath, OriginalName: info.Name, EnrichmentFields: info.EnrichmentFields}

	// update the collection state
	collectionState.Upsert(info)
	return s.OnArtifactDownloaded(ctx, downloadInfo)
}

func (s *AwsS3BucketSource) getClient(ctx context.Context) (*s3.Client, error) {
	var opts []func(*config.LoadOptions) error
	// add credentials if provided
	// TODO #config handle all credential types https://github.com/turbot/tailpipe-plugin-sdk/issues/8
	if s.Config.AccessKey != "" && s.Config.SecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(s.Config.AccessKey, s.Config.SecretKey, s.Config.SessionToken)))
	}

	opts = append(opts, config.WithRegion(typehelpers.SafeString(s.Config.Region)))

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config, %w", err)
	}

	client := s3.NewFromConfig(cfg)
	return client, nil
}
