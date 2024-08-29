package partition

import (
	"context"
	"errors"
	"fmt"

	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// Factory is a global PartitionFactory instance
var Factory = newPartitionFactory()

type PartitionFactory struct {
	// maps of constructors for  the various registered types
	partitionFuncs map[string]func() Partition
	// map of partition schemas
	schemaMap schema.SchemaMap
}

func newPartitionFactory() PartitionFactory {
	return PartitionFactory{
		partitionFuncs: make(map[string]func() Partition),
		schemaMap:      make(schema.SchemaMap),
	}
}

func (f *PartitionFactory) RegisterPartitions(partitionFuncs ...func() Partition) error {
	errs := make([]error, 0)
	for _, ctor := range partitionFuncs {
		// create an instance of the partition to get the identifier
		c := ctor()
		// register the partition
		f.partitionFuncs[c.Identifier()] = ctor

		// get the schema for the partition row type
		rowStruct := c.GetRowSchema()
		s, err := schema.SchemaFromStruct(rowStruct)
		if err != nil {
			errs = append(errs, err)
		}
		// merge in the common schema
		f.schemaMap[c.Identifier()] = s
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (f *PartitionFactory) GetSchema() schema.SchemaMap {
	return f.schemaMap
}

func (f *PartitionFactory) GetPartition(ctx context.Context, req *proto.CollectRequest) (Partition, error) {
	// get the registered constructor for the partition
	ctor, ok := f.partitionFuncs[req.PartitionData.Type]
	if !ok {
		// this type is not registered
		return nil, fmt.Errorf("partition not found: %s", req.PartitionData.Type)
	}

	// create the partition
	col := ctor()

	//  register the partition implementation with the base struct (_before_ calling Init)
	// create an interface type to use - we do not want to expose this function in the Partition interface
	type basePartition interface{ RegisterImpl(Partition) }

	base, ok := col.(basePartition)
	if !ok {
		return nil, fmt.Errorf("partition implementation must embed partition.PartitionBase")
	}
	base.RegisterImpl(col)

	// prepare the data needed for Init

	// convert req into partitionConfigData and sourceConfigData
	partitionConfigData := parse.DataFromProto(req.PartitionData)
	sourceConfigData := parse.DataFromProto(req.SourceData)

	err := col.Init(ctx, partitionConfigData, req.CollectionState, sourceConfigData)
	if err != nil {
		return nil, fmt.Errorf("failed to initialise partition: %w", err)
	}

	return col, nil
}

func (f *PartitionFactory) GetPartitions() map[string]func() Partition {
	return f.partitionFuncs
}
