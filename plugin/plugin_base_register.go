package plugin

import (
	"github.com/turbot/tailpipe-plugin-sdk/collection"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
)

type ResourceFunctions struct {
	Collections []func() collection.Collection
	Sources     []func() row_source.RowSource
}

// RegisterResources registers RowSource implementations
// is should be called by a plugin implementation to register the resources it provides
func (b *PluginBase) RegisterResources(resources *ResourceFunctions) error {
	row_source.Factory.RegisterRowSources(resources.Sources...)
	// RegisterCollections is the only Register function which returns an error
	return collection.Factory.RegisterCollections(resources.Collections...)
}
