package artifact

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	//sdkconfig "github.com/turbot/tailpipe-plugin-sdk/config"
	//"github.com/turbot/tailpipe-plugin-sdk/source"
)

type AwsS3BucketSourceConfig struct {
	Bucket       string
	Prefix       string
	Extensions   []string
	AccessKey    string
	SecretKey    string
	SessionToken string
}

type AwsS3BucketSource struct {
	SourceBase

	Config     *AwsS3BucketSourceConfig
	Extensions types.ExtensionLookup
	TmpDir     string

	//ctx            context.Context
	//observers      []source.SourceObserver
	//observersMutex sync.RWMutex
}

func NewAwsS3BucketSource(config *AwsS3BucketSourceConfig) (*AwsS3BucketSource, error) {
	s := &AwsS3BucketSource{
		Config:     config,
		Extensions: types.NewExtensionLookup(config.Extensions),
	}
	s.TmpDir = path.Join(os.TempDir(), "tailpipe", fmt.Sprintf("s3-%s", config.Bucket))

	if err := s.ValidateConfig(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return s, nil
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
	invalidExtensions := []string{}
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

func (s *AwsS3BucketSource) DiscoverArtifacts(ctx context.Context, req *proto.CollectRequest) error {

	s3Client, err := s.getClient(ctx)
	if err != nil {
		return err
	}

	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: &s.Config.Bucket,
		Prefix: &s.Config.Prefix,
	})

	for paginator.HasMorePages() {
		// TODO send event???

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
				return s.OnArtifactDiscovered(req, info)
			}
		}
	}

	return nil
}

func (s *AwsS3BucketSource) DownloadArtifact(ctx context.Context, req *proto.CollectRequest, info *types.ArtifactInfo) error {
	s3Client, err := s.getClient(ctx)
	if err != nil {
		return err
	}

	// Get the object from S3
	getObjectOutput, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.Config.Bucket,
		Key:    &info.Name,
	})
	if err != nil {
		return fmt.Errorf("failed to download artifact, %w", err)
	}
	defer getObjectOutput.Body.Close()

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

	// Write the data to the local file
	_, err = io.Copy(outFile, getObjectOutput.Body)
	if err != nil {
		return fmt.Errorf("failed to write data to file, %w", err)
	}

	// notify observers of the discovered artifact
	downloadInfo := &types.ArtifactInfo{Name: localFilePath, OriginalName: info.Name}

	return s.OnArtifactDownloaded(req, downloadInfo)

	// TODO WHO WILL DELETE THE TEMP FILES
	return nil
}

func (s *AwsS3BucketSource) getClient(ctx context.Context) (*s3.Client, error) {
	var opts []func(*config.LoadOptions) error
	// add credentials if provided
	if s.Config.AccessKey != "" && s.Config.SecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(s.Config.AccessKey, s.Config.SecretKey, s.Config.SessionToken)))
	}
	// TODO do we need to specify a region>?
	// add with region
	opts = append(opts, config.WithRegion("us-east-1"))

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config, %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	return s3Client, nil
}

//
//func (s *AwsS3BucketSource) getBucketRegion(ctx context.Context, bucketName string) (string, error) {
//		// Load the default configuration
//		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
//		if err != nil {
//			return "", fmt.Errorf("unable to load SDK config, %w", err)
//		}
//
//		// Create an S3 client
//		s3Client := s3.NewFromConfig(cfg)
//
//		// Get the bucket location
//		output, err := s3Client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
//			Bucket: aws.String(bucketName),
//		})
//		if err != nil {
//			return "", fmt.Errorf("unable to get bucket location, %w", err)
//		}
//
//		// Map the location constraint to the region
//		region := string(output.LocationConstraint)
//		if region == "" {
//			region = "us-east-1" // Default region if location constraint is empty
//		}
//
//		return region, nil
//}
//
//
//
