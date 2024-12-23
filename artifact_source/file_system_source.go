package artifact_source

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/elastic/go-grok"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/config_data"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// register the source from the package init function
func init() {
	row_source.RegisterRowSource[*FileSystemSource]()
}

type FileSystemSource struct {
	ArtifactSourceImpl[*artifact_source_config.FileSystemSourceConfig, *EmptyConnection]
	Paths []string
}

func (s *FileSystemSource) Init(ctx context.Context, configData, connectionData config_data.ConfigData, opts ...row_source.RowSourceOption) error {
	// call base to parse config and apply options
	if err := s.ArtifactSourceImpl.Init(ctx, configData, connectionData, opts...); err != nil {
		return err
	}

	s.Paths = s.Config.Paths
	return nil
}

func (s *FileSystemSource) Identifier() string {
	return artifact_source_config.FileSystemSourceIdentifier
}

func (s *FileSystemSource) DiscoverArtifacts(ctx context.Context) error {
	// if we have a layout, check whether this is a directory we should descend into
	layout := s.Config.GetFileLayout()
	filterMap := s.Config.FilterMap
	g := grok.New()
	// add any patterns defined in config
	err := g.AddPatterns(s.Config.GetPatterns())
	if err != nil {
		return fmt.Errorf("error adding grok patterns: %v", err)
	}

	var errList []error
	for _, basePath := range s.Paths {
		err := filepath.WalkDir(basePath, func(targetPath string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			// if we have a layout, check whether this path satisfies the layout and filters
			var metadata map[string][]byte
			var satisfied = true
			if layout != nil {
				// if we are a directory and we are not satisfied, skip the directory by returning fs.SkipDir
				match, metadata, err := s.getPathMetadata(g, basePath, targetPath, layout, d.IsDir())
				if err != nil {
					return err
				}

				// check if the path matches the layout and if so, are filters satisfied
				satisfied = match && MetadataSatisfiesFilters(metadata, filterMap)
			}

			if d.IsDir() {
				// if this is a directory and the pattern is satisfied, descend into the directory
				// (we return nil to continue processing the directory)
				if satisfied {
					return nil
				} else {
					return fs.SkipDir
				}
			}

			// so this is a file
			//if the pattern is not satisfied, skip the file
			if !satisfied {
				return nil
			}

			// get the full path
			absLocation, err := filepath.Abs(targetPath)
			if err != nil {
				return err
			}
			// populate enrichment fields the source is aware of
			// - in this case the source location
			sourceEnrichment := &schema.SourceEnrichment{
				CommonFields: schema.CommonFields{
					TpSourceType:     artifact_source_config.FileSystemSourceIdentifier,
					TpSourceLocation: &absLocation,
				},
				Metadata: metadata,
			}

			artifactInfo := &types.ArtifactInfo{Name: targetPath, SourceEnrichment: sourceEnrichment}
			// notify observers of the discovered artifact
			return s.OnArtifactDiscovered(ctx, artifactInfo)
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

// get the metadata from the given file path, based on the file layout
// returns whether the path matches the layout pattern, and the medata map
func (s *FileSystemSource) getPathMetadata(g *grok.Grok, basePath, targetPath string, layout *string, isDir bool) (bool, map[string][]byte, error) {
	if layout == nil {
		return false, nil, nil
	}
	// remove the base path from the path
	relPath, err := filepath.Rel(basePath, targetPath)
	if err != nil {
		return false, nil, err
	}
	var metadata map[string][]byte
	var match bool
	// if this is a directory, we just want to evaluate the pattern segments up to this directory
	// so call GetPathSegmentMetadata which trims the pattern to match the path length
	if isDir {
		match, metadata, err = GetPathSegmentMetadata(g, relPath, *layout)
	} else {
		match, metadata, err = GetPathMetadata(g, relPath, *layout)
	}
	if err != nil {
		return false, nil, err
	}

	return match, metadata, nil
}

// DownloadArtifact does nothing as the artifact already exists on the local file system
func (s *FileSystemSource) DownloadArtifact(ctx context.Context, info *types.ArtifactInfo) error {
	// notify observers of the discovered artifact
	// NOTE: for now just pass on the info as is
	// if the file was downloaded we would update the Name to the local path, leaving OriginalName as the source path
	// TODO CREATE collection state data https://github.com/turbot/tailpipe-plugin-sdk/issues/11
	return s.OnArtifactDownloaded(ctx, info)
}
