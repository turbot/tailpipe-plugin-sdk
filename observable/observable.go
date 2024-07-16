package observable

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/events"
)

type Observable interface {
	AddObserver(Observer) error
}

// Observer is the interface that all observers must implement
type Observer interface {
	Notify(context.Context, events.Event) error
}
