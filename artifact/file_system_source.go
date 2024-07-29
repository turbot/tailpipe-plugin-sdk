package artifact

import (
	"context"
	"errors"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"os"
	"path/filepath"
)

type FileSystemSourceConfig struct {
	Paths      []string
	Extensions []string
}

type FileSystemSource struct {
	SourceBase
	Paths      []string
	Extensions types.ExtensionLookup
}

func NewFileSystemSource(config *FileSystemSourceConfig) *FileSystemSource {
	return &FileSystemSource{
		Paths:      config.Paths,
		Extensions: types.NewExtensionLookup(config.Extensions),
	}
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
