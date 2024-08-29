package plugin

import (
	"github.com/turbot/tailpipe-plugin-sdk/partition"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
)

type ResourceFunctions struct {
	Partitions []func() partition.Partition
	Sources    []func() row_source.RowSource
}

// RegisterResources registers RowSource implementations
// is should be called by a plugin implementation to register the resources it provides
func (b *PluginBase) RegisterResources(resources *ResourceFunctions) error {
	row_source.Factory.RegisterRowSources(resources.Sources...)
	// RegisterPartitions is the only Register function which returns an error
	return partition.Factory.RegisterPartitions(resources.Partitions...)
}
