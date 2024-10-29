package plugin

import (
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/table"
)

type ResourceFunctions struct {
	Tables  []func() table.Table
	Sources []func() row_source.RowSource
}

// RegisterResources registers RowSource implementations
// is should be called by a plugin implementation to register the resources it provides
func (b *Plugin) RegisterResources(resources *ResourceFunctions) error {
	row_source.Factory.RegisterRowSources(resources.Sources...)
	// RegisterTables is the only Register function which returns an error
	return table.Factory.RegisterTables(resources.Tables...)
}
