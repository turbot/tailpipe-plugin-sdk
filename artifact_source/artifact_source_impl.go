package artifact_source

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/elastic/go-grok"
	"github.com/turbot/pipe-fittings/filter"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/rate_limiter"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"golang.org/x/exp/maps"
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
	// do we want to skip the first row (i.e. for a csv file)
	SkipHeaderRow bool
	Loader        artifact_loader.Loader

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

	// keep track to the time taken for each phase
	DiscoveryTiming types.Timing
	DownloadTiming  types.Timing
	ExtractTiming   types.Timing
	timingLock      sync.Mutex
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
	a.CollectionState.SetGranularity(getGranularityFromFileLayout(a.Config.GetFileLayout()))

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

func (a *ArtifactSourceImpl[S, T]) SetSkipHeaderRow(skipHeaderRow bool) {
	a.SkipHeaderRow = skipHeaderRow
}

// Collect tells our ArtifactSourceImpl to start discovering artifacts
// Implements [plugin.RowSource]
func (a *ArtifactSourceImpl[S, T]) Collect(ctx context.Context) error {
	slog.Info("ArtifactSourceImpl Collect")
	defer slog.Info("ArtifactSourceImpl Collect complete")

	// record discovery start time
	a.DiscoveryTiming.TryStart(constants.TimingDiscover)

	// tell out source to discover artifacts
	// it will notify us of each artifact discovered
	err := a.Source.DiscoverArtifacts(ctx)
	// store discover end time
	a.DiscoveryTiming.End = time.Now()
	if err != nil {
		return err
	}

	// now wait for all extractions
	a.artifactExtractWg.Wait()
	// set extract end time
	a.ExtractTiming.End = time.Now()

	return nil
}

func (a *ArtifactSourceImpl[S, T]) OnArtifactDiscovered(ctx context.Context, info *types.ArtifactInfo) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	// start a download

	// increment the extract wait group - this will be decremented when the artifact is extracted (or there is an error)
	a.artifactExtractWg.Add(1)

	t := time.Now()

	// rate limit the download
	slog.Debug("ArtifactDiscovered - rate limiter waiting", "artifact", info.LocalName)
	err = a.artifactDownloadLimiter.Wait(ctx)
	if err != nil {
		return fmt.Errorf("error acquiring rate limiter: %w", err)
	}
	slog.Debug("ArtifactDiscovered - rate limiter acquired", "duration", time.Since(t), "artifact", info.LocalName)

	// set the download start time if not already set
	a.DownloadTiming.TryStart(constants.TimingDownload)

	go func() {
		defer func() {
			a.artifactDownloadLimiter.Release()
			slog.Debug("ArtifactDiscovered - rate limiter released", "artifact", info.LocalName)
		}()
		downloadStart := time.Now()
		// cast the source to an ArtifactSource and download the artifact
		err = a.Source.DownloadArtifact(ctx, info)
		if err != nil {
			slog.Error("Error downloading artifact", "artifact", info.LocalName, "error", err)
			a.NotifyError(ctx, executionId, err)
		}
		// update the download active duration
		a.DownloadTiming.UpdateActiveDuration(time.Since(downloadStart))
	}()

	// send discovery event
	if err = a.NotifyObservers(ctx, events.NewArtifactDiscoveredEvent(executionId, info)); err != nil {
		return fmt.Errorf("error notifying observers of discovered artifact: %w", err)
	}
	return nil
}

func (a *ArtifactSourceImpl[S, T]) OnArtifactDownloaded(ctx context.Context, info *types.ArtifactInfo) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	// update the download end time
	a.timingLock.Lock()
	a.DownloadTiming.End = time.Now()
	a.timingLock.Unlock()

	// set the extract start time if not already set
	a.ExtractTiming.TryStart(constants.TimingExtract)

	// update the collection state
	if err := a.CollectionState.OnCollected(info); err != nil {
		return fmt.Errorf("error updating collection state: %w", err)
	}

	// extract asynchronously
	go func() {
		extractStart := time.Now()

		// load and extract the artifact
		err := a.processArtifact(ctx, info)

		// update extract active duration
		activeDuration := time.Since(extractStart)
		slog.Debug("ArtifactDownloaded - extract complete", "artifact", info.LocalName, "duration (ms)", activeDuration.Milliseconds())
		a.ExtractTiming.UpdateActiveDuration(activeDuration)

		// close wait group whether there is an error or not
		a.artifactExtractWg.Done()
		if err != nil {
			a.NotifyError(ctx, executionId, err)
		}
	}()

	// notify observers of download
	if err := a.NotifyObservers(ctx, events.NewArtifactDownloadedEvent(executionId, info)); err != nil {
		return fmt.Errorf("error notifying observers of downloaded artifact: %w", err)
	}
	return nil
}

func (a *ArtifactSourceImpl[S, T]) GetTiming() (types.TimingCollection, error) {
	return types.TimingCollection{a.DiscoveryTiming, a.DownloadTiming, a.ExtractTiming}, nil
}

// convert a downloaded artifact to a set of raw rows, with optional metadata
// invoke the artifact loader and any configured mappers to convert the artifact to 'raw' rows,
// which are streamed to the enricher
func (a *ArtifactSourceImpl[S, T]) processArtifact(ctx context.Context, info *types.ArtifactInfo) error {
	slog.Debug("RowSourceImpl processArtifact", "artifact", info.LocalName)

	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	// load artifact data
	// resolve the loader - if one has not been specified, create a default for the file tyoe
	loader, err := a.resolveLoader(info)
	if err != nil {
		return err
	}

	artifactChan := make(chan *types.RowData)
	// load the locally downloaded artifact - decompressing if needed
	err = loader.Load(ctx, info, artifactChan)
	if err != nil {
		return fmt.Errorf("error extracting artifact: %w", err)
	}

	count := 0

	// the loader will return one more more data objects (depending on whether RowPerLine flag is set)
	// range over the data channel and apply extractor if needed
	for artifactData := range artifactChan {

		// raise row events, sending collection state data
		// we may have thousands of notify errors - just store the first one and the count
		var notifyError error
		notifyErrorCount := 0

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
				continue
			}

			if err := a.OnRow(ctx, rawRow); err != nil {
				// store the first error
				if notifyError == nil {
					notifyError = err
				}
				notifyErrorCount++
			}
		}

		if notifyErrorCount > 0 {
			return fmt.Errorf("error notifying %d %s of row event: %w", notifyErrorCount, utils.Pluralize("observer", notifyErrorCount), notifyError)
		}
	}

	// notify observers of extraction (if any rows were extracted)
	if count > 0 {
		if err := a.NotifyObservers(ctx, events.NewArtifactExtractedEvent(executionId, info)); err != nil {
			return fmt.Errorf("error notifying observers of extracted artifact: %w", err)
		}
	}

	// if we skipped the header row, decrement the count to ensure logged row count is accurate
	if a.SkipHeaderRow {
		count--
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
func (a *ArtifactSourceImpl[S, T]) resolveLoader(info *types.ArtifactInfo) (artifact_loader.Loader, error) {
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
func (a *ArtifactSourceImpl[S, T]) WalkNode(ctx context.Context, targetPath string, basePath string, layout *string, isDir bool, g *grok.Grok, filterMap map[string]*filter.SqlFilter) error {
	// if we have a layout, check whether this path satisfies the layout and filters
	var metadata map[string]string
	var satisfied = true

	if layout != nil {
		// if we are a directory and we are not satisfied, skip the directory by returning fs.SkipDir
		var match bool
		var err error
		match, metadata, err = getPathMetadata(targetPath, basePath, layout, isDir, g)
		if err != nil {
			return err
		}

		// check if the path matches the layout and if so, are filters satisfied
		satisfied = match && MetadataSatisfiesFilters(metadata, filterMap)
	}

	if isDir {
		// if this is a directory and the pattern is satisfied, descend into the directory
		// (we return nil to continue processing the directory)
		if satisfied {
			// register this directory with the collection state - it will use the metadata to identify trunks
			a.CollectionState.RegisterPath(targetPath, metadata)
			return nil
		} else {
			return fs.SkipDir
		}
	}

	// so this is a file

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

	// now check with the collection state if we should collect this artifact
	if !a.CollectionState.ShouldCollect(artifactInfo) {
		// do not collect - just return
		return nil
	}

	// so we SHOULD collect -  notify observers of the discovered artifact
	return a.OnArtifactDiscovered(ctx, artifactInfo)
}

// getGranularityFromFileLayout is a helper function to determine the granularity of the collection state based on the file layout
//
// the 'granularity' means what it the shortest period we can determine that an artifact comes from based on its filename
// e.g., if the filename contains {year}/{month}/{day}/{hour}/{minute}, the granularity is 1 minute
// if the filename contains {year}/{month}/{day}/{hour}, the granularity is 1 hour
// NOTE: we traverse the time properties from largest to smallest
func getGranularityFromFileLayout(fileLayout *string) time.Duration {
	if fileLayout == nil {
		return 0
	}

	// get the named capture groups from the regex
	captureGroups := helpers.ExtractNamedGroupsFromGrok(*fileLayout)
	propertyLookup := utils.SliceToLookup(captureGroups)

	slog.Info("getGranularityFromFileLayout", "capture groups", captureGroups, "keys", maps.Keys(propertyLookup))

	// check year/month/day/hour/minute/second
	if _, ok := propertyLookup[constants.TemplateFieldYear]; ok {
		if _, ok := propertyLookup[constants.TemplateFieldMonth]; ok {
			if _, ok := propertyLookup[constants.TemplateFieldDay]; ok {
				if _, ok := propertyLookup[constants.TemplateFieldHour]; ok {
					if _, ok := propertyLookup[constants.TemplateFieldMinute]; ok {
						if _, ok := propertyLookup[constants.TemplateFieldSecond]; ok {
							return time.Second
						}
						return time.Minute
					}
					return time.Hour
				}
				return time.Hour * 24
			}
			return time.Hour * 24 * 30
		}
		return time.Hour * 24 * 365
	}

	return 0
}
