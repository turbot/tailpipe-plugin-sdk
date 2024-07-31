package artifact_row_source

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
	"github.com/turbot/tailpipe-plugin-sdk/artifact_mapper"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"github.com/turbot/tailpipe-plugin-sdk/rate_limiter"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

const ArtifactRowSourceIdentifier = "artifact_row_source"

func init() {
	// register artifact row source
	row_source.Factory.RegisterRowSources(NewArtifactRowSource)
}

// ArtifactRowSource is a [row_source.RowSource] that extracts rows from an 'artifact'
//
// Artifacts are defined as some entity which contains a collection of rows, which must be extracted/processed in
// some way to produce 'raw' rows which can be streamed to a collection. Examples of artifacts include:
// - a gzip file in an S3 bucket
// - a cloudwatch log group
// - a json file on local file system
//
// The ArtifactRowSource is composable, as the same storage location may be used to store different log files in varying formats,
// and the source may need to be configured to know how to extract the log rows from the artifact.
//
// An ArtifactRowSource is composed of:
//   - an [artifact.Source] which discovers and downloads artifacts to a temp local file, and handles incremental/restartable downloads
//   - an [artifact.Loader] which loads the arifact data from the local file, performing any necessary decompression/decryption etc.
//   - optionally, one or more [artifact.Mapper]s which perform processing/conversion/extraction logic required to
//     extract individual data rows from the artifact
//
// The lifetime of the ArtifactRowSource is expected to be the duration of a single collection operation
type ArtifactRowSource struct {
	row_source.Base[ArtifactRowSourceConfig]
	// do we expect the a row to be a line of data
	RowPerLine bool
	Source     artifact_source.Source
	Loader     artifact_loader.Loader
	Mappers    []artifact_mapper.Mapper

	// map of loaders created, keyed by identifier
	// this is populated lazily if we infer the loader from the file type
	loaders    map[string]artifact_loader.Loader
	loaderLock sync.RWMutex

	// rate limiters
	artifactLoadLimiter *rate_limiter.APILimiter

	artifactWg sync.WaitGroup
}

func NewArtifactRowSource() row_source.RowSource {
	return &ArtifactRowSource{
		loaders: make(map[string]artifact_loader.Loader),
	}
}

func (a *ArtifactRowSource) Identifier() string {
	return ArtifactRowSourceIdentifier
}

// TODO #design it is only artifact row source that needs options - maybe just have a different Init function?
func (a *ArtifactRowSource) Init(ctx context.Context, configData *hcl.Data, opts ...row_source.RowSourceOption) error {
	// apply options
	for _, opt := range opts {
		opt(a)
	}

	// call base to apply options and parse config
	if err := a.Base.Init(ctx, configData); err != nil {
		slog.Warn("Initializing ArtifactRowSource failed", "error", err)
		return err
	}
	slog.Info("Initialized ArtifactRowSource", "config", a.Config)

	// ok so we now have parsed config. We need to create new config data ro pass to the artifact source factory
	artifactSourceConfigData := &hcl.Data{
		// set source type
		Type: a.Config.Source,
		// copy the other fields
		// TODO use unknonw HCL!!
		ConfigData: configData.ConfigData,
		Filename:   configData.Filename,
		Pos:        configData.Pos,
	}

	slog.Info("Creating artifact source", "source type", a.Config.Source)
	artifactSource, err := artifact_source.Factory.GetArtifactSource(ctx, artifactSourceConfigData)
	if err != nil {
		return err
	}
	a.Source = artifactSource

	// NOTE: see if the source requires a mapper
	// (e.g. if the source is a Cloudwatch source, we need to add a mapper to extract the cloudtrail metadata)
	// if so, this mapper is put at the START of the chain
	if mapperFunc := artifactSource.Mapper(); mapperFunc != nil {
		a.Mappers = append([]artifact_mapper.Mapper{mapperFunc()}, a.Mappers...)
	}

	// TODO #design think about this - it;s a bit funky that both the artifact source AND the row source store the paging data
	//  - but this is because we store it in the artifact_row_source.Base
	// now we have created the source, we can set the paging data
	// set the paging data to be an empty data struct (as returned by GetPagingDataSchema)
	a.PagingData = a.GetPagingDataSchema()

	// add ourselves as observer to a
	if err = artifactSource.AddObserver(a); err != nil {
		return err
	}

	// setup rate limiter
	a.artifactLoadLimiter = rate_limiter.NewAPILimiter(&rate_limiter.Definition{
		Name: "artifact_load_limiter",
		// TODO #config #debug set to one for simplicity for now
		MaxConcurrency: 1,
	})

	return nil
}

// GetPagingDataSchema should be overriden by the RowSource implementation to return the paging data schema
// base implementation returns nil
func (a *ArtifactRowSource) GetPagingDataSchema() paging.Data {
	return a.Source.GetPagingDataSchema()
}

// SetPagingData overrides Base.SetPagingData to pass the paging data to the source
func (a *ArtifactRowSource) SetPagingData(pagingDataJSON json.RawMessage) error {
	err := a.Base.SetPagingData(pagingDataJSON)
	if err != nil {
		return err
	}
	// pass paging data to the source
	a.Source.SetPagingData(a.PagingData)
	return nil
}

// Close the source
// Implements [plugin.RowSource]
func (a *ArtifactRowSource) Close() error {
	slog.Debug("Closing ArtifactRowSource")
	// close the source
	return a.Source.Close()
}

// Notify  handles all events which the ArtifactRowSource expects to receive (ArtifactDiscovered, ArtifactDownloaded)
// implements [observable.Observer]
func (a *ArtifactRowSource) Notify(ctx context.Context, event events.Event) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	switch e := event.(type) {
	case *events.ArtifactDiscovered:
		// increment the wait group - this weill be decremented when the artifact is extracted (or there is an error
		a.artifactWg.Add(1)
		slog.Debug("ArtifactDiscovered - rate limiter waiting", "artifact", e.Info.Name)
		t := time.Now()

		// rate limit the download
		err := a.artifactLoadLimiter.Wait(ctx)
		if err != nil {
			return fmt.Errorf("error acquiring rate limiter: %w", err)
		}
		slog.Debug("ArtifactDiscovered - rate limiter acquired", "duration", time.Since(t), "artifact", e.Info.Name)

		go func() {
			defer func() {
				a.artifactLoadLimiter.Release()
				slog.Debug("ArtifactDiscovered - rate limiter released", "artifact", e.Info.Name)
			}()

			err = a.Source.DownloadArtifact(ctx, e.Info)

			if err != nil {
				a.artifactWg.Done()
				err := a.NotifyObservers(ctx, events.NewErrorEvent(executionId, err))
				if err != nil {
					slog.Error("Error notifying observers of download error", "download error", err, "notify error", err)
				}
			}
		}()
	case *events.ArtifactDownloaded:
		// update our paging data with the paging data from the this artifact event
		a.updatePagingData(e.PagingData)

		//extract
		go func() {
			// TODO #err make sure errors handles and bubble back
			err := a.extractArtifact(ctx, e.Info)
			// close wait group whether there is an error or not
			a.artifactWg.Done()
			if err != nil {
				err := a.NotifyObservers(ctx, events.NewErrorEvent(executionId, err))
				if err != nil {
					slog.Error("Error notifying observers of extract error", "extract error", err, "notify error", err)
				}
			}
		}()
	default:
		// TODO just ignore?
		return fmt.Errorf("ArtifactRowSource does not handle event type: %T", e)
	}
	return nil
}

// Collect tells our ArtifactRowSource to start discovering artifacts
// Implements [plugin.RowSource]
func (a *ArtifactRowSource) Collect(ctx context.Context) error {
	// tell out source to discover artifacts
	// it will notify us of each artifact discovered
	if err := a.Source.DiscoverArtifacts(ctx); err != nil {
		return err
	}
	// now wait for all extractions
	a.artifactWg.Wait()

	return nil
}

func (a *ArtifactRowSource) SetLoader(loader artifact_loader.Loader) {
	a.Loader = loader
}

func (a *ArtifactRowSource) AddMappers(mappers ...artifact_mapper.Mapper) {
	a.Mappers = append(a.Mappers, mappers...)
}

func (a *ArtifactRowSource) SetRowPerLine(rowPerLine bool) {
	a.RowPerLine = rowPerLine
}

// convert a downloaded artifact to a set of raw rows, with optional metadata
// invoke the artifact loader and any configured mappers to convert the artifact to 'raw' rows,
// which are streamed to the enricher
func (a *ArtifactRowSource) extractArtifact(ctx context.Context, info *types.ArtifactInfo) error {
	// load artifact data
	// resolve the loader - if one has not been specified, create a default for the file tyoe
	loader, err := a.resolveLoader(info)
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
	for artifactData := range artifactChan {
		// pout data into an array as that is what mappers expect
		rows, err := a.mapArtifacts(ctx, artifactData)

		if err != nil {
			return fmt.Errorf("error mapping artifact: %w", err)
		}
		// raise row events, sending paging data
		var notifyErrors []error
		for _, row := range rows {
			count++
			// NOTE: if no metadata has been set on the row, use any metadata from the artifact
			if row.Metadata == nil {
				row.Metadata = info.EnrichmentFields
			}
			if err := a.OnRow(ctx, row, a.PagingData); err != nil {
				notifyErrors = append(notifyErrors, err)
			}
		}
		if len(notifyErrors) > 0 {
			return fmt.Errorf("error notifying %d %s of row event: %w", len(notifyErrors), utils.Pluralize("observer", len(notifyErrors)), errors.Join(notifyErrors...))
		}
	}

	slog.Debug("ArtifactRowSource extractArtifact complete", "artifact", info.Name, "rows", count)
	return nil
}

// mapArtifacts applies any configured mappers to the artifact data
func (a *ArtifactRowSource) mapArtifacts(ctx context.Context, artifactData *types.RowData) ([]*types.RowData, error) {
	// mappers may return multiple rows so wrap data in a list
	var dataList = []*types.RowData{artifactData}

	// iff there are no mappers, just return the data as is
	if len(a.Mappers) == 0 {
		return []*types.RowData{artifactData}, nil
	}
	var errList []error

	// invoke each mapper in turn
	for _, m := range a.Mappers {
		var mappedDataList []*types.RowData
		for _, d := range dataList {
			mappedData, err := m.Map(ctx, d)
			if err != nil {
				// TODO #err should we give up immediately
				errList = append(errList, err)
			} else {
				mappedDataList = append(mappedDataList, mappedData...)
			}
		}
		// update artifactData list
		dataList = mappedDataList
	}

	if len(errList) > 0 {
		return nil, fmt.Errorf("error mapping artifact rows: %w", errors.Join(errList...))
	}

	return dataList, nil
}

// resolveLoader resolves the loader to use for the artifact
// - if a loader has been specified, just use that
// - otherwise create a default loader based on the extension
func (a *ArtifactRowSource) resolveLoader(info *types.ArtifactInfo) (artifact_loader.Loader, error) {

	// a loader was specified when creating the row source - use that
	if a.Loader != nil {
		return a.Loader, nil
	}

	var key string
	var ctor func() artifact_loader.Loader
	// figure out which loader to use based on the file extension
	switch filepath.Ext(info.Name) {
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

func (a *ArtifactRowSource) updatePagingData(data paging.Data) {
	if data == nil {
		return
	}
	if a.PagingData == nil {
		a.PagingData = data
		return
	}
	a.PagingData.Update(data)
}
