package artifact_source

import (
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io"
	"log/slog"
	"os"
	"path"
)

const (
	GcpStorageBucketSourceIdentifier = "gcp_storage_bucket"
)

func init() {
	// register source
	row_source.Factory.RegisterRowSources(NewGcpStorageBucketSource)
}

// GcpStorageBucketSource is a [ArtifactSource] implementation that reads artifacts from a GCP Storage bucket
type GcpStorageBucketSource struct {
	ArtifactSourceBase[*GcpStorageBucketSourceConfig]

	Config     GcpStorageBucketSourceConfig
	Extensions types.ExtensionLookup
	client     *storage.Client
}

func NewGcpStorageBucketSource() row_source.RowSource {
	return &GcpStorageBucketSource{}
}

func (s *GcpStorageBucketSource) Init(ctx context.Context, configData *hcl.Data, opts ...row_source.RowSourceOption) error {
	// call base to parse config and apply options
	if err := s.ArtifactSourceBase.Init(ctx, configData, opts...); err != nil {
		return err
	}

	s.TmpDir = path.Join(BaseTmpDir, fmt.Sprintf("gcp-storage-%s", s.Config.Bucket))
	s.Extensions = types.NewExtensionLookup(s.Config.Extensions)

	client, err := s.getClient(ctx)
	if err != nil {
		return err
	}
	s.client = client

	slog.Info("Initialized GcpStorageBucketSource", "bucket", s.Config.Bucket, "extensions", s.Extensions)
	return nil
}

func (s *GcpStorageBucketSource) Identifier() string {
	return GcpStorageBucketSourceIdentifier
}

func (s *GcpStorageBucketSource) GetConfigSchema() hcl.Config {
	return &GcpStorageBucketSourceConfig{}
}

func (s *GcpStorageBucketSource) Close() error {
	return s.client.Close()
}

func (s *GcpStorageBucketSource) DiscoverArtifacts(ctx context.Context) error {
	bucket := s.client.Bucket(s.Config.Bucket)
	query := &storage.Query{Prefix: s.Config.Prefix}

	objectIterator := bucket.Objects(ctx, query)
	for {
		obj, err := objectIterator.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				break
			} else {
				return fmt.Errorf("failed to list objects in bucket: %s", err.Error())
			}
		}
		objPath := obj.Name
		if s.Extensions.IsValid(objPath) {
			sourceEnrichmentFields := &enrichment.CommonFields{
				TpSourceLocation: &objPath,
				TpSourceName:     s.Config.Bucket, // TODO: verify with Kai this is correct place to populate source name
			}

			info := &types.ArtifactInfo{Name: objPath, OriginalName: objPath, EnrichmentFields: sourceEnrichmentFields}

			if err := s.OnArtifactDiscovered(ctx, info); err != nil {
				// TODO: #error should we continue or fail?
				return fmt.Errorf("failed to notify observers of discovered artifact, %w", err)
			}
		}
	}
	return nil
}

func (s *GcpStorageBucketSource) DownloadArtifact(ctx context.Context, info *types.ArtifactInfo) error {
	bucket := s.client.Bucket(s.Config.Bucket)
	obj := bucket.Object(info.Name)

	reader, err := obj.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("failed to get object reader: %s", err.Error())
	}
	defer reader.Close()

	localFilePath := path.Join(s.TmpDir, info.Name)
	if err := os.MkdirAll(path.Dir(localFilePath), 0755); err != nil {
		return fmt.Errorf("failed to create directory for file, %w", err)
	}

	outFile, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("failed to create file, %w", err)
	}
	defer outFile.Close()

	if _, err := io.Copy(outFile, reader); err != nil {
		return fmt.Errorf("failed to write data to file, %w", err)
	}

	downloadInfo := &types.ArtifactInfo{Name: localFilePath, OriginalName: info.Name}

	// TODO: #paging create paging data https://github.com/turbot/tailpipe-plugin-sdk/issues/13
	pagingInfo := paging.NewStorageBucket()
	return s.OnArtifactDownloaded(ctx, downloadInfo, pagingInfo)
}

func (s *GcpStorageBucketSource) getClient(ctx context.Context) (*storage.Client, error) {
	opts, err := s.setSessionConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed setting GCP Storage client config: %s", err.Error())
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP Storage client: %s", err.Error())
	}

	return client, nil
}

func (s *GcpStorageBucketSource) setSessionConfig(ctx context.Context) ([]option.ClientOption, error) {
	var opts []option.ClientOption

	// Credentials
	if s.Config.Credentials != nil {
		credentials, err := s.pathOrContents(*s.Config.Credentials)
		if err != nil {
			return nil, fmt.Errorf("failed to read credentials file: %s", err.Error())
		}

		opts = append(opts, option.WithCredentialsJSON([]byte(credentials)))
	}

	// Quota Project
	quotaProject := os.Getenv("GOOGLE_CLOUD_QUOTA_PROJECT")

	if s.Config.QuotaProject != nil {
		quotaProject = *s.Config.QuotaProject
	}

	if quotaProject != "" {
		opts = append(opts, option.WithQuotaProject(quotaProject))
	}

	// Impersonate
	if s.Config.Impersonate != nil {
		ts, err := impersonate.CredentialsTokenSource(ctx, impersonate.CredentialsConfig{
			TargetPrincipal: *s.Config.Impersonate,
			Scopes:          []string{"https://www.googleapis.com/auth/cloud-platform"},
		})
		if err != nil {
			return opts, err
		}

		opts = append(opts, option.WithTokenSource(ts))
	}

	return opts, nil
}

// TODO: Determine where this actually belongs, maybe a useful util func? https://github.com/turbot/tailpipe-plugin-sdk/issues/14
func (s *GcpStorageBucketSource) pathOrContents(in string) (string, error) {
	if len(in) == 0 {
		return "", nil
	}

	filePath := in

	if filePath[0] == '~' {
		var err error
		filePath, err = homedir.Expand(filePath)
		if err != nil {
			return filePath, err
		}
	}

	if _, err := os.Stat(filePath); err == nil {
		contents, err := os.ReadFile(filePath)
		if err != nil {
			return string(contents), err
		}
		return string(contents), nil
	}

	if len(filePath) > 1 && (filePath[0] == '/' || filePath[0] == '\\') {
		return "", fmt.Errorf("%s: no such file or dir", filePath)
	}

	return in, nil
}
