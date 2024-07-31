package artifact_source

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

func init() {
	// register source
	Factory.RegisterArtifactSources(NewFileSystemSource)
}

type FileSystemSource struct {
	SourceBase
	Paths      []string
	Extensions types.ExtensionLookup
}

func NewFileSystemSource() Source {
	return &FileSystemSource{}
}

func (s *FileSystemSource) Init(ctx context.Context, configData *hcl.Data) error {
	// parse the config
	var c, _, err = hcl.ParseConfig[FileSystemSourceConfig](configData)
	if err != nil {
		slog.Error("Error parsing config", "error", err)
		return err
	}

	s.Paths = c.Paths
	s.Extensions = types.NewExtensionLookup(c.Extensions)
	slog.Info("Initialized FileSystemSource", "paths", s.Paths, "extensions", s.Extensions)
	return nil
}

func (s *FileSystemSource) Identifier() string {
	return FileSystemSourceIdentifier
}

func (s *FileSystemSource) DiscoverArtifacts(ctx context.Context) error {
	// TODO async????

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

	// TODO consider large/remote files/download progress
	//s.NotifyObservers(events.NewArtifactDownloadProgress(request, info))

	// notify observers of the discovered artifact
	// NOTE: for now just pass on the info as is
	// if the file was downloaded we would update the Name to the local path, leaving OriginalName as the source path
	// TODO CREATE PAGING DATA
	return s.OnArtifactDownloaded(ctx, info, nil)
}
