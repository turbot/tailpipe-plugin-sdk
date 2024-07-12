package observable

import "github.com/turbot/tailpipe-plugin-sdk/events"

type Observable interface {
	AddObserver(Observer) error
}

// Observer is the interface that all observers must implement
type Observer interface {
	Notify(events.Event) error
}
