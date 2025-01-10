package artifact_source

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// register the source from the package init function
func init() {
	row_source.RegisterRowSource[*PluginSourceWrapper]()
}

// PluginSourceWrapper is an implementation of ArtifactSource which wraps a GRPC plugin which implements the source
// all RowSource implementations delegate to the plugin, while the remainder of the ArtifactSource operations:
// loading, extraction are handled by the base ArtifactSourceImpl
type PluginSourceWrapper struct {
	// NOTE: we are using the plugin source for ArtifactsSource operations (i.e. downloading the artifacts),
	// the ArtifactSourceImpl handles the remaining operations (loading/extraction)
	// We still need to parameterise the ArtifactSourceImpl, however we just pass empty config and connection -
	// the implementation of the RowSource in the plugin will handle the config and connection
	// (we pass the raw config and connection to the plugin)
	ArtifactSourceImpl[*NilArtifactSourceConfig, *NilConfig]
	client     *grpc.PluginClient
	pluginName string
	sourceType string
	// the collection state json returned by the plugin
	collectionStateJSON json.RawMessage
	// wait group to wait for the external plugin source to complete
	sourceWg    sync.WaitGroup
	executionId string
}

// Init is called when the row source is created
// it is responsible for parsing the source config and configuring the source
func (w *PluginSourceWrapper) Init(ctx context.Context, params *row_source.RowSourceParams, opts ...row_source.RowSourceOption) error {
	// apply options
	for _, opt := range opts {
		if err := opt(w); err != nil {
			return err
		}
	}

	// create a NilArtifactCollectionState - this will do nothing but is required to avoid
	// nil reference exceptions in ArtifactSourceImpl.OnArtifactDownloaded
	w.CollectionState = &NilArtifactCollectionState{}

	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	w.executionId = executionId

	// the source config data should contain a reattach config
	if params.SourceConfigData.ReattachConfig == nil {
		return fmt.Errorf("PluginSourceWrapper requires a reattach config")
	}
	// create the plugin client
	err = w.SetPlugin(params.SourceConfigData.ReattachConfig)
	if err != nil {
		return err
	}
	// now call into the source plugin to initialise the source
	req := &proto.InitSourceRequest{
		SourceParams: params.AsProto(),
	}

	if w.defaultConfig != nil {
		req.DefaultConfig = w.defaultConfig.AsProto()
	}

	_, err = w.client.InitSource(req)

	return err
}

func (w *PluginSourceWrapper) SaveCollectionState() error {
	_, err := w.client.SaveCollectionState()
	return err
}

// SetPlugin sets the plugin client for the source
// this is called from WithPluginReattach option
func (w *PluginSourceWrapper) SetPlugin(sourcePlugin *types.SourcePluginReattach) error {
	client, err := grpc.NewPluginClientFromReattach(sourcePlugin)
	if err != nil {
		return err
	}
	w.client = client
	w.pluginName = sourcePlugin.Plugin
	w.sourceType = sourcePlugin.SourceType
	return nil
}

// AddObserver adds an observer to the source (overriding the base implementation)
func (w *PluginSourceWrapper) AddObserver(o observable.Observer) error {
	// use base implementation to add the observer
	err := w.ArtifactSourceImpl.AddObserver(o)
	if err != nil {
		return err
	}
	// get the event stream from the plugin
	eventStream, err := w.client.AddObserver()
	if err != nil {
		return err
	}

	// add the execution id to the context
	ctx := context_values.WithExecutionId(context.Background(), w.executionId)

	// start a goroutine to read the eventStream and listen to file events
	// this will loop until it hits an error or the stream is closed
	go w.readSourceEvents(ctx, eventStream)

	return nil
}

// Identifier must return the source name
func (w *PluginSourceWrapper) Identifier() string {
	return row_source.PluginSourceWrapperIdentifier
}

// Description returns a human readable description of the source
func (w *PluginSourceWrapper) Description() (string, error) {
	// this may be called before client is set
	if w.client == nil {
		return "Plugin source wrapper", nil
	}

	res, err := w.client.Describe()
	if err != nil {
		return "", err
	}
	source, ok := res.Sources[w.sourceType]
	if !ok {
		return "", fmt.Errorf("source %s not found in plugin", w.sourceType)
	}

	return source.Description, nil
}

func (w *PluginSourceWrapper) Close() error {
	_, err := w.client.CloseSource()
	w.client.Client.Kill()
	return err
}

// Collect is called to start collecting data,
func (w *PluginSourceWrapper) Collect(ctx context.Context) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	// signal the wait group that the source is running
	w.sourceWg.Add(1)

	_, err = w.client.SourceCollect(
		&proto.SourceCollectRequest{
			ExecutionId: executionId,
		},
	)
	if err != nil {
		return err
	}

	// wait for the source to complete - this will be cleared by the event handler
	// TODO timeout?
	w.sourceWg.Wait()
	// also wait for any artifact extractions to complete
	w.artifactExtractWg.Wait()
	return nil
}

// GetCollectionStateJSON returns the json serialised collection state data for the ongoing collection
func (w *PluginSourceWrapper) GetCollectionStateJSON() (json.RawMessage, error) {
	return w.collectionStateJSON, nil
}

// GetTiming returns the timing for the source row collection
func (w *PluginSourceWrapper) GetTiming() (types.TimingCollection, error) {
	resp, err := w.client.GetSourceTiming()
	if err != nil {
		return types.TimingCollection{}, nil
	}
	return events.TimingCollectionFromProto(resp.Timing), nil

}

func (w *PluginSourceWrapper) readSourceEvents(ctx context.Context, pluginStream proto.TailpipePlugin_AddObserverClient) {
	pluginEventChan := make(chan *proto.Event)
	errChan := make(chan error)

	// goroutine to read the plugin event stream and send the events down the event channel
	go func() {
		defer func() {
			if r := recover(); r != nil {
				errChan <- helpers.ToError(r)
			}
		}()

		for {
			e, err := pluginStream.Recv()
			if err != nil {
				errChan <- err
				return
			}
			pluginEventChan <- e
		}
	}()

	// loop until the context is cancelled
	// TODO think about cancellation/other completion scenarios https://github.com/turbot/tailpipe/issues/8
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errChan:
			if err != nil {
				w.NotifyError(ctx, w.executionId, fmt.Errorf("error reading from plugin stream: %v", err))
				return
			}
		case protoEvent := <-pluginEventChan:
			// convert the protobuff event to an observer event
			// and send it to the observer
			if protoEvent == nil {
				w.NotifyError(ctx, w.executionId, fmt.Errorf("nil event received from plugin"))
				return
			}

			// TODO #error should we quit if we get an error event?

			switch protoEvent.Event.(type) {
			case *proto.Event_ArtifactDownloadedEvent:
				// increment the wait group - this would normally be done in OnArtifactDiscovered
				// but we are not calling that as the plugin source is doing the discovery and downloading
				// We need to increment it here as OnArtifactDownloaded will decrement it
				w.artifactExtractWg.Add(1)
				// set the collection state
				w.collectionStateJSON = protoEvent.GetArtifactDownloadedEvent().CollectionState
				// get artifact info from the event
				artifactInfo := types.ArtifactInfoFromProto(protoEvent.GetArtifactDownloadedEvent().ArtifactInfo)
				err := w.OnArtifactDownloaded(ctx, artifactInfo)
				if err != nil {
					w.NotifyError(ctx, w.executionId, err)
				}

			case *proto.Event_SourceCompleteEvent:
				close(pluginEventChan)
				// close wait group
				w.sourceWg.Done()
				return
			default:
				// pass all other events onwards
				// convert to a observable event
				ev, err := events.SourceEventFromProto(protoEvent)
				if err != nil {
					w.NotifyError(ctx, w.executionId, err)
					continue
				}
				err = w.NotifyObservers(ctx, ev)
				if err != nil {
					w.NotifyError(ctx, w.executionId, err)
					continue
				}
			}
		}
	}
}
