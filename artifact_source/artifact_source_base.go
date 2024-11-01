package artifact_source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/rate_limiter"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// TODO #config configure https://github.com/turbot/tailpipe-plugin-sdk/issues/4
const ArtifactSourceMaxConcurrency = 16

// ArtifactSourceBase is a [row_source.RowSource] that extracts rows from an 'artifact'
//
// Artifacts are defined as some entity which contains a collection of rows, which must be extracted/processed in
// some way to produce 'raw' rows which can be streamed to a collection. Examples of artifacts include:
// - a gzip file in an S3 bucket
// - a cloudwatch log group
// - a json file on local file system
//
// The ArtifactSourceBase is composable, as the same storage location may be used to store different log files in varying formats,
// and the source may need to be configured to know how to extract the log rows from the artifact.
//
// An ArtifactSourceBase is composed of:
//   - an [artifact.ArtifactSource] which discovers and downloads artifacts to a temp local file, and handles incremental/restartable downloads
//   - an [artifact.Loader] which loads the arifact data from the local file, performing any necessary decompression/decryption etc.
//   - optionally, one or more [artifact.Mapper]s which perform processing/conversion/extraction logic required to
//     extract individual data rows from the artifact
//
// The lifetime of the ArtifactSourceBase is expected to be the duration of a single collection operation
type ArtifactSourceBase[T artifact_source_config.ArtifactSourceConfig] struct {
	row_source.RowSourceBase[T]
	// do we expect the a row to be a line of data
	RowPerLine bool
	Loader     artifact_loader.Loader

	// TODO #config should this be in base - means the risk that a derived struct will not set it https://github.com/turbot/tailpipe-plugin-sdk/issues/3
	TmpDir string

	// shadow the row_source.RowSourceBase Impl property, but using ArtifactSource interface
	Impl ArtifactSource

	defaultConfig *artifact_source_config.ArtifactSourceConfigBase
	// map of loaders created, keyed by identifier
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

func (b *ArtifactSourceBase[T]) SetRowPerLine(rowPerLine bool) {
	b.RowPerLine = rowPerLine
}

func (b *ArtifactSourceBase[T]) Init(ctx context.Context, configData *types.ConfigData, opts ...row_source.RowSourceOption) error {
	slog.Info("Initializing ArtifactSourceBase")

	// if no collection state func has been set by a derived struct,
	// set it to the default for artifacts
	if b.NewCollectionStateFunc == nil {
		b.NewCollectionStateFunc = collection_state.NewArtifactCollectionState
	}

	// call base to apply options and parse config
	if err := b.RowSourceBase.Init(ctx, configData, opts...); err != nil {
		slog.Warn("Initializing artifact_row_source.RowSourceBase failed", "error", err)
		return err
	}

	slog.Info("Initialized artifact_row_source.RowSourceBase", "config", b.Config)
	slog.Info("Default to default config", "defaultConfig", b.defaultConfig)

	// apply default config (this handles null default)
	b.Config.DefaultTo(b.defaultConfig)

	// store RowSourceBase.Impl as an ArtifactSource
	impl, ok := b.RowSourceBase.Impl.(ArtifactSource)
	if !ok {
		return errors.New("ArtifactSourceBase.Impl must implement ArtifactSource")
	}
	b.Impl = impl

	// setup rate limiter
	b.artifactDownloadLimiter = rate_limiter.NewAPILimiter(&rate_limiter.Definition{
		Name:           "artifact_load_limiter",
		MaxConcurrency: ArtifactSourceMaxConcurrency,
	})

	// create loader map
	b.loaders = make(map[string]artifact_loader.Loader)

	// initialise the collection state
	return b.CollectionState.Init(b.Config)
}

// Collect tells our ArtifactSourceBase to start discovering artifacts
// Implements [plugin.RowSource]
func (b *ArtifactSourceBase[T]) Collect(ctx context.Context) error {
	slog.Info("ArtifactSourceBase Collect")
	defer slog.Info("ArtifactSourceBase Collect complete")

	// record discovery start time
	b.DiscoveryTiming.TryStart(constants.TimingDiscover)

	// tell out source to discover artifacts
	// it will notify us of each artifact discovered
	err := b.Impl.DiscoverArtifacts(ctx)
	// store discover end time
	b.DiscoveryTiming.End = time.Now()
	if err != nil {
		return err
	}

	// now wait for all extractions
	b.artifactExtractWg.Wait()
	// set extract end time
	b.ExtractTiming.End = time.Now()

	return nil
}

func (b *ArtifactSourceBase[T]) SetLoader(loader artifact_loader.Loader) {
	b.Loader = loader
}

func (b *ArtifactSourceBase[T]) SetDefaultConfig(config *artifact_source_config.ArtifactSourceConfigBase) {
	b.defaultConfig = config
}

func (b *ArtifactSourceBase[T]) OnArtifactDiscovered(ctx context.Context, info *types.ArtifactInfo) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	// start a download

	// increment the extract wait group - this will be decremented when the artifact is extracted (or there is an error)
	b.artifactExtractWg.Add(1)

	t := time.Now()

	// rate limit the download
	slog.Debug("ArtifactDiscovered - rate limiter waiting", "artifact", info.Name)
	err = b.artifactDownloadLimiter.Wait(ctx)
	if err != nil {
		return fmt.Errorf("error acquiring rate limiter: %w", err)
	}
	slog.Debug("ArtifactDiscovered - rate limiter acquired", "duration", time.Since(t), "artifact", info.Name)

	// set the download start time if not already set
	b.DownloadTiming.TryStart(constants.TimingDownload)

	go func() {
		defer func() {
			b.artifactDownloadLimiter.Release()
			slog.Debug("ArtifactDiscovered - rate limiter released", "artifact", info.Name)
		}()
		downloadStart := time.Now()
		// cast the source to an ArtifactSource and download the artifact
		err = b.Impl.DownloadArtifact(ctx, info)
		if err != nil {
			slog.Error("Error downloading artifact", "artifact", info.Name, "error", err)
			// we failed to download artifact so decrement the wait group
			b.artifactExtractWg.Done()
			err := b.NotifyObservers(ctx, events.NewErrorEvent(executionId, err))
			if err != nil {
				slog.Error("Error notifying observers of download error", "download error", err, "notify error", err)
			}
		}
		// update the download active duration
		b.DownloadTiming.UpdateActiveDuration(time.Since(downloadStart))
	}()

	// send discovery event
	if err = b.NotifyObservers(ctx, events.NewArtifactDiscoveredEvent(executionId, info)); err != nil {
		return fmt.Errorf("error notifying observers of discovered artifact: %w", err)
	}
	return nil
}

func (b *ArtifactSourceBase[T]) OnArtifactDownloaded(ctx context.Context, info *types.ArtifactInfo) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	// update the download end time
	b.timingLock.Lock()
	b.DownloadTiming.End = time.Now()
	b.timingLock.Unlock()

	// set the extract start time if not already set
	b.ExtractTiming.TryStart(constants.TimingExtract)

	// serialise the collection state data so that we capture it at the time of the download of this artifact
	collectionState, err := b.GetCollectionStateJSON()
	if err != nil {
		return fmt.Errorf("error serialising collection state data: %w", err)
	}

	// extract asynchronously
	go func() {
		extractStart := time.Now()
		// TODO #error make sure errors handles and bubble back
		err := b.extractArtifact(ctx, info, collectionState)

		// update extract active duration
		activeDuration := time.Since(extractStart)
		slog.Debug("ArtifactDownloaded - extract complete", "artifact", info.Name, "duration (ms)", activeDuration.Milliseconds())
		b.ExtractTiming.UpdateActiveDuration(activeDuration)

		slog.Debug("ArtifactDownloaded - extract complete", "artifact", info.Name)

		// close wait group whether there is an error or not
		b.artifactExtractWg.Done()
		if err != nil {
			err := b.NotifyObservers(ctx, events.NewErrorEvent(executionId, err))
			if err != nil {
				slog.Error("Error notifying observers of extract error", "extract error", err, "notify error", err)
			}
		}
	}()

	// notify observers of download
	if err := b.NotifyObservers(ctx, events.NewArtifactDownloadedEvent(executionId, info, collectionState)); err != nil {
		return fmt.Errorf("error notifying observers of downloaded artifact: %w", err)
	}
	return nil
}

func (b *ArtifactSourceBase[T]) GetTiming() types.TimingCollection {
	return types.TimingCollection{b.DiscoveryTiming, b.DownloadTiming, b.ExtractTiming}
}

// convert a downloaded artifact to a set of raw rows, with optional metadata
// invoke the artifact loader and any configured mappers to convert the artifact to 'raw' rows,
// which are streamed to the enricher
func (b *ArtifactSourceBase[T]) extractArtifact(ctx context.Context, info *types.ArtifactInfo, collectionState json.RawMessage) error {
	slog.Debug("RowSourceBase extractArtifact", "artifact", info.Name)

	executionId, err := context_values.ExecutionIdFromContext(ctx)
	// load artifact data
	// resolve the loader - if one has not been specified, create a default for the file tyoe
	loader, err := b.resolveLoader(info)
	if err != nil {
		return err
	}

	artifactChan := make(chan *types.RowData)
	err = loader.Load(ctx, info, artifactChan)
	if err != nil {
		return fmt.Errorf("error extracting artifact: %w", err)
	}

	count := 0

	// range over the data channel and apply mappers
	for rawRow := range artifactChan {
		// raise row events, sending collection state data
		var notifyErrors []error
		count++
		// NOTE: if no metadata has been set on the row, use any metadata from the artifact
		if rawRow.Metadata == nil {
			rawRow.Metadata = info.EnrichmentFields
		}
		if err := b.OnRow(ctx, rawRow, collectionState); err != nil {
			notifyErrors = append(notifyErrors, err)
		}

		if len(notifyErrors) > 0 {
			return fmt.Errorf("error notifying %d %s of row event: %w", len(notifyErrors), utils.Pluralize("observer", len(notifyErrors)), errors.Join(notifyErrors...))
		}
	}

	// notify observers of download
	if err := b.NotifyObservers(ctx, events.NewArtifactExtractedEvent(executionId, info)); err != nil {
		return fmt.Errorf("error notifying observers of extracted artifact: %w", err)
	}

	slog.Debug("RowSourceBase extractArtifact complete", "artifact", info.Name, "rows", count)
	return nil
}

// resolveLoader resolves the loader to use for the artifact
// - if a loader has been specified, just use that
// - otherwise create a default loader based on the extension
func (b *ArtifactSourceBase[T]) resolveLoader(info *types.ArtifactInfo) (artifact_loader.Loader, error) {
	// a loader was specified when creating the row source - use that
	if b.Loader != nil {
		return b.Loader, nil
	}

	var key string
	var ctor func() artifact_loader.Loader
	// figure out which loader to use based on the file extension
	switch filepath.Ext(info.Name) {
	case ".gz":
		if b.RowPerLine {
			key = artifact_loader.GzipRowLoaderIdentifier
			ctor = artifact_loader.NewGzipRowLoader
		} else {
			key = artifact_loader.GzipLoaderIdentifier
			ctor = artifact_loader.NewGzipLoader
		}
	default:
		if b.RowPerLine {
			key = artifact_loader.FileRowLoaderIdentifier
			ctor = artifact_loader.NewFileRowLoader
		} else {
			key = artifact_loader.FileLoaderIdentifier
			ctor = artifact_loader.NewFileLoader
		}
	}

	// have we already created this loader?
	b.loaderLock.RLock()
	l, ok := b.loaders[key]
	b.loaderLock.RUnlock()
	if ok {
		// yes, return it
		return l, nil
	}

	// no - create and cache a new one
	// upgrade the lock
	b.loaderLock.Lock()
	defer b.loaderLock.Unlock()

	// check the map again (in case of race condition)
	if l, ok = b.loaders[key]; ok {
		return l, nil
	}

	// so we do need to create
	l = ctor()

	// store
	b.loaders[key] = l

	return l, nil
}

// functions which must be implemented by structs embedding ArtifactSourceBase

func (b *ArtifactSourceBase[T]) Identifier() string {
	panic("Identifier must be implemented by the ArtifactSource implementation")
}

func (b *ArtifactSourceBase[T]) GetConfigSchema() parse.Config {
	panic("GetConfigSchema must be implemented by the ArtifactSource implementation")
}

func (b *ArtifactSourceBase[T]) DiscoverArtifacts(ctx context.Context) error {
	panic("DiscoverArtifacts must be implemented by the ArtifactSource implementation")
}

func (b *ArtifactSourceBase[T]) DownloadArtifact(ctx context.Context, info *types.ArtifactInfo) error {
	panic("DownloadArtifact must be implemented by the ArtifactSource implementation")
}
