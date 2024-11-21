package artifact_source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/config_data"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// register the source from the package init function
func init() {
	row_source.RegisterRowSource[*GcpStorageBucketSource]()
}

// GcpStorageBucketSource is a [ArtifactSource] implementation that reads artifacts from a GCP Storage bucket
type GcpStorageBucketSource struct {
	ArtifactSourceImpl[*artifact_source_config.GcpStorageBucketSourceConfig, *GcpConnection]

	Extensions types.ExtensionLookup
	client     *storage.Client
}

func (s *GcpStorageBucketSource) Init(ctx context.Context, configData, connectionData config_data.ConfigData, opts ...row_source.RowSourceOption) error {
	// call base to parse config and apply options
	if err := s.ArtifactSourceImpl.Init(ctx, configData, connectionData, opts...); err != nil {
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
	return artifact_source_config.GcpStorageBucketSourceIdentifier
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
				TpSourceName:     &s.Config.Bucket,
				TpSourceType:     artifact_source_config.GcpStorageBucketSourceIdentifier,
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

	downloadInfo := &types.ArtifactInfo{Name: localFilePath, OriginalName: info.Name, EnrichmentFields: info.EnrichmentFields}

	// TODO: #delta create collection state data https://github.com/turbot/tailpipe-plugin-sdk/issues/13
	return s.OnArtifactDownloaded(ctx, downloadInfo)
}

func (s *GcpStorageBucketSource) getClient(ctx context.Context) (*storage.Client, error) {
	opts, err := s.Connection.GetClientOptions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed setting GCP Storage client config: %s", err.Error())
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP Storage client: %s", err.Error())
	}

	return client, nil
}
