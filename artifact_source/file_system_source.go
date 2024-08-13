package artifact_source

import (
	"context"
	"errors"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

const (
	FileSystemSourceIdentifier = "file_system"
)

func init() {
	// register source
	row_source.Factory.RegisterRowSources(NewFileSystemSource)
}

type FileSystemSource struct {
	ArtifactSourceBase[*FileSystemSourceConfig]
	Paths      []string
	Extensions types.ExtensionLookup
}

func NewFileSystemSource() row_source.RowSource {
	return &FileSystemSource{}
}

func (s *FileSystemSource) Init(ctx context.Context, configData *hcl.Data, opts ...row_source.RowSourceOption) error {
	// call base to parse config and apply options
	if err := s.ArtifactSourceBase.Init(ctx, configData, opts...); err != nil {
		return err
	}

	s.Paths = s.Config.Paths
	s.Extensions = types.NewExtensionLookup(s.Config.Extensions)
	slog.Info("Initialized FileSystemSource", "paths", s.Paths, "extensions", s.Extensions)
	return nil
}

func (s *FileSystemSource) Identifier() string {
	return FileSystemSourceIdentifier
}

func (s *FileSystemSource) GetConfigSchema() hcl.Config {
	return &FileSystemSourceConfig{}
}

func (s *FileSystemSource) DiscoverArtifacts(ctx context.Context) error {
	var errList []error
	for _, path := range s.Paths {
		err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}

			// check the extension
			if s.Extensions.IsValid(path) {
				// populate enrichment fields the the source is aware of
				// - in this case the source location
				sourceEnrichmentFields := &enrichment.CommonFields{
					TpSourceLocation: &path,
				}

				info := &types.ArtifactInfo{Name: path, EnrichmentFields: sourceEnrichmentFields}
				// notify observers of the discovered artifact
				return s.OnArtifactDiscovered(ctx, info)
			}
			return nil
		})
		if err != nil {
			errList = append(errList, err)
		}

	}
	if len(errList) > 0 {
		return errors.Join(errList...)
	}
	return nil
}

func (s *FileSystemSource) DownloadArtifact(ctx context.Context, info *types.ArtifactInfo) error {

	// TODO consider large/remote files/download progress https://github.com/turbot/tailpipe-plugin-sdk/issues/10
	//s.NotifyObservers(events.NewArtifactDownloadProgress(request, info))

	// notify observers of the discovered artifact
	// NOTE: for now just pass on the info as is
	// if the file was downloaded we would update the Name to the local path, leaving OriginalName as the source path
	// TODO CREATE PAGING DATA https://github.com/turbot/tailpipe-plugin-sdk/issues/11
	return s.OnArtifactDownloaded(ctx, info)
}
