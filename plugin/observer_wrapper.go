package plugin

import (
	"context"
	"golang.org/x/time/rate"
	"log/slog"

	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

// ObserverWrapper mapd between proto Observer and the plugin Observer
type ObserverWrapper struct {
	protoObserver proto.TailpipePlugin_AddObserverServer
	limiter       *rate.Limiter
}

func NewObserverWrapper(protoObserver proto.TailpipePlugin_AddObserverServer) ObserverWrapper {
	return ObserverWrapper{
		protoObserver: protoObserver,
		limiter:       rate.NewLimiter(1000, 1),
	}
}

// Notify implements the Observer interface but sends to a proto stream
func (o ObserverWrapper) Notify(ctx context.Context, e events.Event) error {
	if err := o.limiter.Wait(ctx); err != nil {
		return err
	}
	if p, ok := e.(events.ProtoEvent); ok {
		err := o.protoObserver.Send(p.ToProto())
		if err != nil {
			slog.Error("Error sending event to observer", "event", e, "error", err)
			return err
		}
	}
	// this event does not implement ProtoEvent, so do not send over protobuf
	return nil
}
