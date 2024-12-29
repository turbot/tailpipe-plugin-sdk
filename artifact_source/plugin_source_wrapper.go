package artifact_source

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// an empty config implementation as we need to ste connection and config type for the PluginSourceWrapper
// the config and conneciton will not be used

type NilConfig struct{}

func (n NilConfig) Validate() error {
	return nil
}

func (n NilConfig) Identifier() string {
	return "empty_config"
}

type NilArtifactSourceConfig struct{}

func (n NilArtifactSourceConfig) Validate() error {
	return nil
}

func (n NilArtifactSourceConfig) Identifier() string {
	return "empty_artifact_source_config"
}

func (n NilArtifactSourceConfig) GetFileLayout() *string {
	return nil
}

func (n NilArtifactSourceConfig) DefaultTo(_ artifact_source_config.ArtifactSourceConfig) {
}

type PluginSourceWrapper struct {
	ArtifactSourceImpl[*NilArtifactSourceConfig, *NilConfig]
	client     *SourcePluginClient
	pluginName string
	sourceType string
	observer   observable.Observer
}

func NewPluginSourceWrapper(sourcePlugin *types.SourcePluginReattach) (row_source.RowSource, error) {
	client, err := NewPluginClientFromReattach(sourcePlugin)
	if err != nil {
		return nil, err
	}
	s := &PluginSourceWrapper{
		client:     client,
		pluginName: sourcePlugin.Plugin,
		sourceType: sourcePlugin.SourceType,
	}

	return s, nil
}

func (w *PluginSourceWrapper) AddObserver(o observable.Observer) error {
	w.observer = o
	eventStream, err := w.client.AddObserver()

	if err != nil {
		return err
	}
	// start a goroutine to read the eventStream and listen to file events
	// this will loop until it hits an error or the stream is closed
	go w.readSourceEvents(context.Background(), eventStream)

	return nil
}

// Init is called when the row source is created
// it is responsible for parsing the source config and configuring the source
func (w *PluginSourceWrapper) Init(ctx context.Context, configData types.ConfigData, connectionData types.ConfigData, opts ...row_source.RowSourceOption) error {
	for _, opt := range opts {
		err := opt(w)
		if err != nil {
			return err
		}
	}

	return w.client.Init(ctx, configData, connectionData, opts...)
}

// Identifier must return the source name
func (w *PluginSourceWrapper) Identifier() string {
	return w.sourceType
}

// Description returns a human readable description of the source
func (w *PluginSourceWrapper) Description() (string, error) {
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
	err := w.client.Close()
	// TODO K correct?
	w.client.client.Kill()
	return err
}

// Collect is called to start collecting data,
func (w *PluginSourceWrapper) Collect(ctx context.Context) error {
	return w.client.Collect(ctx)
}

// GetCollectionStateJSON returns the json serialised collection state data for the ongoing collection
func (w *PluginSourceWrapper) GetCollectionStateJSON() (json.RawMessage, error) {
	return w.client.GetCollectionStateJSON()
}

// SetCollectionStateJSON unmarshalls the collection state data JSON into the target object
func (w *PluginSourceWrapper) SetCollectionStateJSON(stateJSON json.RawMessage) error {
	return w.client.SetCollectionStateJSON(stateJSON)
}

// GetTiming returns the timing for the source row collection
func (w *PluginSourceWrapper) GetTiming() types.TimingCollection {
	return w.client.GetTiming()
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
				// TODO #error WHAT TO DO HERE? send error to observers

				fmt.Printf("Error reading from plugin stream: %v\n", err)
				return
			}
		case protoEvent := <-pluginEventChan:
			// convert the protobuff event to an observer event
			// and send it to the observer
			if protoEvent == nil {
				// TODO #error unexpected - raise an error - send error to observers
				return
			}
			err := w.observer.Notify(ctx, events.EventFromProto(protoEvent))
			if err != nil {
				fmt.Printf("Error notifying observer: %v\n", err)
				// TODO #error WHAT TO DO HERE? send error to observers
				continue
			}
			// TODO #error should we quit if we get an error event?
			// if this is a completion event (or other error event???), stop polling
			if protoEvent.GetCompleteEvent() != nil {
				close(pluginEventChan)
				return
			}
		}
	}

}
