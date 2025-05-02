package artifact_source

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/elastic/go-grok"
	"github.com/turbot/pipe-fittings/v2/filter"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/rate_limiter"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

const ArtifactSourceMaxConcurrency = 16

// ArtifactSourceImpl is a [row_source.RowSource] that extracts rows from an 'artifact'
//
// Artifacts are defined as some entity which contains a collection of rows, which must be extracted/processed in
// some way to produce 'raw' rows which can be streamed to a collection. Examples of artifacts include:
// - a gzip file in an S3 bucket
// - a cloudwatch log group
// - a json file on local file system
//
// The ArtifactSourceImpl is composable, as the same storage location may be used to store different log files in varying formats,
// and the source may need to be configured to know how to extract the log rows from the artifact.
//
// An ArtifactSourceImpl is composed of:
//   - an [artifact.ArtifactSource] which discovers and downloads artifacts to a temp local file, and handles incremental/restartable downloads
//   - an [artifact.Loader] which loads the arifact data from the local file, performing any necessary decompression/decryption etc.
//   - optionally, one or more [artifact.Mapper]s which perform processing/conversion/extraction logic required to
//     extract individual data rows from the artifact
//
// The lifetime of the ArtifactSourceImpl is expected to be the duration of a single collection operation
type ArtifactSourceImpl[S artifact_source_config.ArtifactSourceConfig, T parse.Config] struct {
	row_source.RowSourceImpl[S, T]

	// do we expect the a row to be a line of data
	RowPerLine bool
	// is there a header row we want to skip the first row (i.e. for a csv file)
	SkipHeaderRow bool
	// what is the delimiter for the header row
	// (if this is set, a header event will be raised for the header of each file)
	HeaderRowDelimiter string

	Loader artifact_loader.Loader

	// temporary directory for storing downloaded artifacts - this is initialised in the Init function
	// to be a subdirectory of the collection directory
	TempDir string

	// shadow the row_source.RowSourceImpl Source property, but using ArtifactSource interface
	Source ArtifactSource

	// shadow the CollectionState property, but using ArtifactCollectionStateImpl
	CollectionState collection_state.ArtifactCollectionState[S]

	defaultConfig *artifact_source_config.ArtifactSourceConfigImpl
	// map of loaders created, keyed by identifier
	// an optional extractor which the table may specify
	extractor Extractor

	// this is populated lazily if we infer the loader from the file type
	loaders    map[string]artifact_loader.Loader
	loaderLock sync.RWMutex

	// rate limiters
	artifactDownloadLimiter *rate_limiter.APILimiter

	// wait group to wait for all artifacts to be extracted
	// this is incremented each time we discover an artifact and decremented when we have extracted it
	artifactExtractWg sync.WaitGroup
}

func (a *ArtifactSourceImpl[S, T]) Init(ctx context.Context, params *row_source.RowSourceParams, opts ...row_source.RowSourceOption) error {
	slog.Info("Initializing ArtifactSourceImpl", "configData", params.SourceConfigData.GetHcl())

	// if no collection state func has been set by a derived struct,
	// set it to the default for artifacts
	if a.NewCollectionStateFunc == nil {
		a.NewCollectionStateFunc = collection_state.NewArtifactCollectionStateImpl
	}

	// set the temp directory
	a.TempDir = filepath.Join(params.CollectionTempDir, "artifacts")

	// call base to apply options and parse config
	if err := a.RowSourceImpl.Init(ctx, params, opts...); err != nil {
		slog.Warn("Initializing artifact_row_source.RowSourceImpl failed", "error", err)
		return err
	}

	slog.Info("Initialized artifact_row_source.RowSourceImpl", "config", a.Config)
	slog.Info("Default to default config", "defaultConfig", a.defaultConfig)

	// apply default artifact config (this handles null default)
	a.Config.DefaultTo(a.defaultConfig)

	// store RowSourceImpl.Source as an ArtifactSource (shadow the base Source property)
	impl, ok := a.RowSourceImpl.Source.(ArtifactSource)
	if !ok {
		return errors.New("ArtifactSourceImpl.Source must implement ArtifactSource")
	}
	a.Source = impl

	// store the collection state as an ArtifactCollectionState (shadow the base CollectionState property)
	cs, ok := any(a.RowSourceImpl.CollectionState).(collection_state.ArtifactCollectionState[S])
	if !ok {
		return errors.New("ArtifactSourceImpl.CollectionState must implement ArtifactCollectionState")
	}
	a.CollectionState = cs

	// set the granularity
	a.CollectionState.SetGranularity(helpers.GetGranularityFromFileLayout(a.Config.GetFileLayout()))

	// setup rate limiter
	a.artifactDownloadLimiter = rate_limiter.NewAPILimiter(&rate_limiter.Definition{
		Name:           "artifact_load_limiter",
		MaxConcurrency: ArtifactSourceMaxConcurrency,
	})

	return nil
}

func (a *ArtifactSourceImpl[S, T]) SetLoader(loader artifact_loader.Loader) {
	a.Loader = loader
}

// options functions

// SetExtractor sets the extractor function for the source
func (a *ArtifactSourceImpl[S, T]) SetExtractor(extractor Extractor) {
	a.extractor = extractor
}

// SetDefaultConfig sets the default config for the source
func (a *ArtifactSourceImpl[S, T]) SetDefaultConfig(config *artifact_source_config.ArtifactSourceConfigImpl) {
	a.defaultConfig = config
}

func (a *ArtifactSourceImpl[S, T]) SetRowPerLine(rowPerLine bool) {
	a.RowPerLine = rowPerLine
}

// SetSkipHeaderRow sets the skip header row flag for the source, but does not set the delimiter.
// The header row will be skipped but no Header event will be raised
func (a *ArtifactSourceImpl[S, T]) SetSkipHeaderRow() {
	a.SkipHeaderRow = true
}

// SetHeaderDelimiter sets the skip header row flag for the source, and sets the delimiter used to split the header.
// The header row will be skipped and a Header event will be raised with split header.
// This header will then be used by the collector to pass to all MapRow calls for that artifact
func (a *ArtifactSourceImpl[S, T]) SetHeaderDelimiter(delimiter string) {
	a.SkipHeaderRow = true
	a.HeaderRowDelimiter = delimiter
}

// Collect tells our ArtifactSourceImpl to start discovering artifacts
// Implements [plugin.RowSource]
func (a *ArtifactSourceImpl[S, T]) Collect(ctx context.Context) error {
	slog.Info("ArtifactSourceImpl Collect")
	defer slog.Info("ArtifactSourceImpl Collect complete")

	// tell out source to discover artifacts
	// it will notify us of each artifact discovered
	err := a.Source.DiscoverArtifacts(ctx)
	if err != nil {
		return err
	}

	// now wait for all extractions
	a.artifactExtractWg.Wait()

	return nil
}

func (a *ArtifactSourceImpl[S, T]) OnArtifactDiscovered(ctx context.Context, info *types.ArtifactInfo) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	slog.Info("ArtifactDiscovered", "artifact", info.Name)
	// start a download

	// increment the extract wait group - this will be decremented when the artifact is extracted (or there is an error)
	a.artifactExtractWg.Add(1)

	t := time.Now()

	// rate limit the download
	slog.Debug("ArtifactDiscovered - rate limiter waiting", "artifact", info.Name)
	err = a.artifactDownloadLimiter.Wait(ctx)
	if err != nil {
		return fmt.Errorf("error acquiring rate limiter: %w", err)
	}
	slog.Debug("ArtifactDiscovered - rate limiter acquired", "duration", time.Since(t), "artifact", info.Name)

	go func() {
		defer func() {
			a.artifactDownloadLimiter.Release()
			slog.Debug("ArtifactDiscovered - rate limiter released", "artifact", info.Name)
		}()

		slog.Info("ArtifactSourceImpl OnArtifactDiscovered - waiting for pause", "artifact", info.Name)
		// as this is called from the file walking code, rather than as a result of an event,
		// we need to check for pausing here to avoid downloading artifacts when paused
		a.BlockWhilePaused(ctx)
		slog.Info("ArtifactSourceImpl OnArtifactDiscovered - AFTER pause", "artifact", info.Name)

		// cast the source to an ArtifactSource and download the artifact
		err = a.Source.DownloadArtifact(ctx, info)
		if err != nil {
			// if the downloading failed, we will not have called OnArtifactDownloaded so the wait group will not be decremented
			a.artifactExtractWg.Done()
			slog.Error("Error downloading artifact", "artifact", info.Name, "error", err)
			a.NotifyError(ctx, executionId, err)
		}
	}()

	// send discovery event
	if err = a.NotifyObservers(ctx, events.NewArtifactDiscoveredEvent(executionId, info)); err != nil {
		return fmt.Errorf("error notifying observers of discovered artifact: %w", err)
	}
	return nil
}

func (a *ArtifactSourceImpl[S, T]) OnArtifactDownloaded(ctx context.Context, info *types.DownloadedArtifactInfo) (err error) {
	// if we have a Null loader, do not start the goroutine to process the artifact
	nullLoader := a.hasNullLoader()

	slog.Info("ArtifactDownloaded", "artifact", info.Name, "nullLoader", nullLoader)
	// if we have a null loader and there is no error, we need to decrement the wait group here
	// (if there is an error the calling code will decrement it)
	defer func() {
		if nullLoader && err == nil {
			a.artifactExtractWg.Done()
		}
	}()

	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	// update the collection state
	if err := a.CollectionState.OnCollected(info.Identifier(), info.Timestamp); err != nil {
		return fmt.Errorf("error updating collection state: %w", err)
	}

	// if we DO NOT have a null loader, start the go routine to process the artifact
	// (if we have a null loader, we must have a ArtifactConversionCollector which will do the processing)
	if !nullLoader {
		// extract asynchronously
		go func() {
			extractStart := time.Now()

			// load and extract the artifact
			err := a.processArtifact(ctx, info)

			// update extract active duration
			activeDuration := time.Since(extractStart)
			slog.Debug("ArtifactDownloaded - extraction complete", "artifact", info.LocalName, "duration (ms)", activeDuration.Milliseconds())

			// close wait group whether there is an error or not
			a.artifactExtractWg.Done()

			if err != nil {
				slog.Error("error processing artifact", "artifact", info.Name, "error", err)
				a.NotifyError(ctx, executionId, err)
			}
		}()
	}

	// notify observers of download
	if err := a.NotifyObservers(ctx, events.NewArtifactDownloadedEvent(executionId, info)); err != nil {
		slog.Error("error processing artifact", "artifact", info.Name, "error", err)
		a.NotifyError(ctx, executionId, fmt.Errorf("error processing artifact: %w", err))
	}

	return nil
}

// convert a downloaded artifact to a set of raw rows, with optional metadata
// invoke the artifact loader and any configured mappers to convert the artifact to 'raw' rows,
// which are streamed to the enricher
func (a *ArtifactSourceImpl[S, T]) processArtifact(ctx context.Context, info *types.DownloadedArtifactInfo) error {
	slog.Debug("RowSourceImpl processArtifact", "artifact", info.LocalName)

	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	// load artifact data
	// resolve the loader
	loader, err := a.resolveLoader(info)
	if err != nil {
		return err
	}

	artifactChan := make(chan *types.RowData)
	// load the locally downloaded artifact - decompressing if needed
	err = loader.Load(ctx, info, artifactChan)
	if err != nil {
		return fmt.Errorf("%s: loading failed: %w", info.Name, err)
	}

	var count int64 = 0

	// the loader will return one or more data objects (depending on whether RowPerLine flag is set)
	// range over the data channel and apply extractor if needed
	for artifactData := range artifactChan {

		// add source enrichment from the artifacts to the artifact data
		artifactData.SourceEnrichment = info.SourceEnrichment

		// if an extractor was specified by the table, apply it
		rawRaws, err := a.extractRowsFromArtifact(ctx, artifactData)
		if err != nil {
			return err
		}

		for _, rawRow := range rawRaws {
			count++

			// if we're skipping the header row, skip the first row
			// (note: as we already incremented count we check for 1)
			if a.SkipHeaderRow && count == 1 {
				// raise an event with the header, in case anyone downstream needs it
				// (for example a mapper which uses the header to build a format)
				if err := a.onHeader(ctx, info, rawRow); err != nil {
					return fmt.Errorf("error processing header row: %w", err)
				}
				continue
			}

			// errors from OnRow are non-fatal and already handled in RowSourceImpl
			_ = a.OnRow(ctx, rawRow)
		}
	}

	// if we skipped the header row, decrement the count to ensure logged row count is accurate
	if a.SkipHeaderRow {
		count--
	}

	// notify observers of extraction (if any rows were extracted)
	if count > 0 {
		if err := a.NotifyObservers(ctx, events.NewArtifactExtractedEvent(executionId, info, count)); err != nil {
			return fmt.Errorf("error notifying observers of extracted artifact: %w", err)
		}
	}

	slog.Debug("RowSourceImpl processArtifact complete", "artifact", info.LocalName, "rows", count)

	return nil
}

// if an extractor is specified, apply it to the artifact data to extract rows
func (a *ArtifactSourceImpl[S, T]) extractRowsFromArtifact(ctx context.Context, artifactData *types.RowData) ([]*types.RowData, error) {
	// if no extractor is set, nothing to do
	if a.extractor == nil {
		// just return the artifact data as a single row
		return []*types.RowData{artifactData}, nil
	}
	// TODO #errors error here results in wg negative error
	var res []*types.RowData
	rows, err := a.extractor.Extract(ctx, artifactData.Data)
	if err != nil {
		return nil, fmt.Errorf("error extracting rows: %w", err)
	}

	// convert the rows to an array of RowData
	for _, row := range rows {
		res = append(res, &types.RowData{
			Data:             row,
			SourceEnrichment: artifactData.SourceEnrichment,
		})
	}
	return res, nil
}

// resolveLoader resolves the loader to use for the artifact
// - if a loader has been specified, just use that
// - otherwise create a default loader based on the extension
func (a *ArtifactSourceImpl[S, T]) resolveLoader(info *types.DownloadedArtifactInfo) (artifact_loader.Loader, error) {
	// a loader was specified when creating the row source - use that
	if a.Loader != nil {
		return a.Loader, nil
	}

	// create map if needed
	if a.loaders == nil {
		a.loaders = make(map[string]artifact_loader.Loader)
	}

	var key string
	var ctor func() artifact_loader.Loader
	// figure out which loader to use based on the file extension
	switch filepath.Ext(info.LocalName) {
	case ".gz":
		if a.RowPerLine {
			key = artifact_loader.GzipRowLoaderIdentifier
			ctor = artifact_loader.NewGzipRowLoader
		} else {
			key = artifact_loader.GzipLoaderIdentifier
			ctor = artifact_loader.NewGzipLoader
		}
	default:
		if a.RowPerLine {
			key = artifact_loader.FileRowLoaderIdentifier
			ctor = artifact_loader.NewFileRowLoader
		} else {
			key = artifact_loader.FileLoaderIdentifier
			ctor = artifact_loader.NewFileLoader
		}
	}

	// have we already created this loader?
	a.loaderLock.RLock()
	l, ok := a.loaders[key]
	a.loaderLock.RUnlock()
	if ok {
		// yes, return it
		return l, nil
	}

	// no - create and cache a new one
	// upgrade the lock
	a.loaderLock.Lock()
	defer a.loaderLock.Unlock()

	// check the map again (in case of race condition)
	if l, ok = a.loaders[key]; ok {
		return l, nil
	}

	// so we do need to create
	l = ctor()

	// store
	a.loaders[key] = l

	return l, nil
}

// functions which must be implemented by structs embedding ArtifactSourceImpl

func (a *ArtifactSourceImpl[S, T]) Identifier() string {
	panic("Identifier must be implemented by the ArtifactSource implementation")
}

func (a *ArtifactSourceImpl[S, T]) DiscoverArtifacts(ctx context.Context) error {
	panic("DiscoverArtifacts must be implemented by the ArtifactSource implementation")
}

func (a *ArtifactSourceImpl[S, T]) DownloadArtifact(ctx context.Context, info *types.ArtifactInfo) error {
	panic("DownloadArtifact must be implemented by the ArtifactSource implementation")
}

// WalkNode is called for each file or directory discovered by the file source - it is called as part of the folder
// walking discovery algorithm
func (a *ArtifactSourceImpl[S, T]) WalkNode(ctx context.Context, targetPath string, basePath string, layouts []string, isDir bool, g *grok.Grok, filterMap map[string]*filter.SqlFilter) error {
	// apply the file layout pattern and filters to determine whether this path matches, and iff so, extract metadata
	metadata, satisfied, err := a.getMetadataAndApplyFilters(targetPath, basePath, layouts, isDir, g, filterMap)
	if err != nil {
		return err
	}

	if isDir {
		return a.walkDirNode(targetPath, metadata, satisfied)
	}

	// so this is a file
	return a.walkFileNode(ctx, targetPath, satisfied, metadata)
}

func (a *ArtifactSourceImpl[S, T]) getMetadataAndApplyFilters(targetPath string, basePath string, layouts []string, isDir bool, g *grok.Grok, filterMap map[string]*filter.SqlFilter) (map[string]string, bool, error) {
	// if the original file layout had any optional segments, we will have expanded them into multiple potential layouts
	// try each one and use the first one which matches
	var match bool
	var metadata map[string]string
	var err error
	for _, layout := range layouts {
		// check whether this path satisfies the layout and filters

		// if we are a directory and we are not satisfied, skip the directory by returning fs.SkipDir
		match, metadata, err = getPathMetadata(targetPath, basePath, layout, isDir, g)
		if err != nil {
			return nil, false, err
		}
		if match {
			break
		}
	}

	// check if the path matches the layout and if so, are filters satisfied
	satisfied := match && metadataSatisfiesFilters(metadata, filterMap)

	// if we have a from time, check whether that excludes this directory
	if satisfied && isDir && !a.FromTime.IsZero() {
		satisfied = dirSatisfiesFromTime(a.FromTime, metadata)
	}

	return metadata, satisfied, nil
}

func (a *ArtifactSourceImpl[S, T]) walkFileNode(ctx context.Context, targetPath string, satisfied bool, metadata map[string]string) error {
	// if the pattern is not satisfied, skip the file
	if !satisfied {
		return nil
	}

	// so we are satisfied - determine whether we should collect this artifact

	// populate enrichment that fields the source is aware of
	// - in this case the source location
	// add to metadata - Common fields will be populated from it
	metadata["tp_source_location"] = targetPath
	metadata["tp_source_type"] = a.Source.Identifier()

	// build the source enrichment
	sourceEnrichment := schema.NewSourceEnrichment(metadata)

	// create an artifact info - this will parse the timestamp of the artifact from the source enrichment metadata
	artifactInfo, err := types.NewArtifactInfo(targetPath, sourceEnrichment, a.CollectionState.GetGranularity())
	if err != nil {
		return err
	}

	// if the artifact has a timestamp, and  we have a from time, check if the artifact is newer than the from time
	if !artifactInfo.Timestamp.IsZero() && !a.FromTime.IsZero() {
		if artifactInfo.Timestamp.Compare(a.FromTime) < 0 {
			return nil
		}
	}

	// now check with the collection state if we should collect this artifact
	if !a.CollectionState.ShouldCollect(artifactInfo.Identifier(), artifactInfo.Timestamp) {
		// do not collect - just return
		return nil
	}

	// so we SHOULD collect -  notify observers of the discovered artifact
	return a.OnArtifactDiscovered(ctx, artifactInfo)
}

func (a *ArtifactSourceImpl[S, T]) walkDirNode(targetPath string, metadata map[string]string, satisfied bool) error {
	// if this is a directory and the pattern is satisfied, descend into the directory
	// (we return nil to continue processing the directory)
	if satisfied {
		// register this directory with the collection state - it will use the metadata to identify trunks
		a.CollectionState.RegisterPath(targetPath, metadata)
		return nil
	}

	return fs.SkipDir
}

func (a *ArtifactSourceImpl[S, T]) hasNullLoader() bool {
	return a.Loader != nil && a.Loader.Identifier() == artifact_loader.NullLoaderIdentifier
}

// OnHeader is called for the first row when  when extracting an artifact with the SkipHeaderRow param set
func (a *ArtifactSourceImpl[S, T]) onHeader(ctx context.Context, info *types.DownloadedArtifactInfo, row *types.RowData) error {
	// if not delimiter is set, that means WithSkipHeaderRow option was passed rather WithHeader
	// the plugin does not need to be notified, so just return
	if a.HeaderRowDelimiter == "" {
		return nil
	}

	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	headerString, ok := row.Data.(string)
	if !ok {
		return fmt.Errorf("header row is not a string")
	}
	// split the header row into columns
	header := strings.Split(headerString, a.HeaderRowDelimiter)
	return a.NotifyObservers(ctx, events.NewHeaderEvent(executionId, &info.ArtifactInfo, header))
}
