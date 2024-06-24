package observable

import "github.com/turbot/tailpipe-plugin-sdk/events"

// Observer is the interface that all observers must implement
type Observer interface {
	Notify(events.Event) error
}
