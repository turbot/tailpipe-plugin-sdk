package row_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/observable"
)

// RowSourceOption is a function that can be used to configure a RowSource
// NOTE: individual options are specific to specific row source types
// RowSourceOption accepts the base Observable interface,
// and each option must implement a safe type assertion to the specific row source type
type RowSourceOption func(observable.Observable)
