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
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

func init() {
	// register source
	Factory.RegisterArtifactSources(NewAwsS3BucketSource)
}

// AwsS3BucketSource is a [Source] implementation that reads artifacts from an S3 bucket
type AwsS3BucketSource struct {
	SourceBase

	Config     AwsS3BucketSourceConfig
	Extensions types.ExtensionLookup
	client     *s3.Client
}

func NewAwsS3BucketSource() Source {
	return &AwsS3BucketSource{}
}

func (s *AwsS3BucketSource) Init(ctx context.Context, configData *hcl.Data) error {
	// parse the config
	var c, _, err = hcl.ParseConfig[AwsS3BucketSourceConfig](configData)
	if err != nil {
		slog.Error("AwsS3BucketSource Init - error parsing config", "error", err)
		return err
	}

	s.Config = c
	s.Extensions = types.NewExtensionLookup(c.Extensions)

	if err := s.ValidateConfig(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	s.TmpDir = path.Join(c.TmpDir, "tailpipe", fmt.Sprintf("s3-%s", c.Bucket))

	if err := s.ValidateConfig(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
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
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: &s.Config.Bucket,
		Prefix: &s.Config.Prefix,
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
					TpSourceLocation: &path,
				}

				info := &types.ArtifactInfo{Name: path, EnrichmentFields: sourceEnrichmentFields}
				// notify observers of the discovered artifact
				if err := s.OnArtifactDiscovered(ctx, info); err != nil {
					// TODO #err should we continue or fail?
					return fmt.Errorf("failed to notify observers of discovered artifact, %w", err)
				}
			}
		}
	}

	return nil
}

func (s *AwsS3BucketSource) DownloadArtifact(ctx context.Context, info *types.ArtifactInfo) error {
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
	downloadInfo := &types.ArtifactInfo{Name: localFilePath, OriginalName: info.Name}

	// create paging data
	// TODO #paging
	// figure out s3 paging
	paging := paging.NewS3Bucket()
	return s.OnArtifactDownloaded(ctx, downloadInfo, paging)
}

func (s *AwsS3BucketSource) getClient(ctx context.Context) (*s3.Client, error) {
	var opts []func(*config.LoadOptions) error
	// add credentials if provided
	// TODO handle all credential types
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

	client := s3.NewFromConfig(cfg)
	return client, nil
}
