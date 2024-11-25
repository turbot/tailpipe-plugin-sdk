package table

import (
	"errors"
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// Factory is a global TableFactory instance
var Factory = newTableFactory()

// RegisterTable registers a table constructor with the factory
// this is called from the package init function of the table implementation
func RegisterTable[R types.RowStruct, S parse.Config, T Table[R, S]]() {
	tableFunc := func() Collector {
		return &Partition[R, S, T]{
			table: utils.InstanceOf[T](),
		}
	}

	Factory.registerTable(tableFunc)
}

type TableFactory struct {
	partitionFuncs []func() Collector
	// maps of partition constructors, keyed by the name of the registered table types
	partitionFuncMap map[string]func() Collector
}

func newTableFactory() TableFactory {
	return TableFactory{
		partitionFuncMap: make(map[string]func() Collector),
	}
}

// registerTable just store the constructor in an array
// This will be called before the Init function is called
// Init creates instances of each table - these ar eused to get the table identifier
// (for the map key) and the schema
// we defer this until TableFactory.Init as registerTable is called from
// package init functions which cannot return an error
func (f *TableFactory) registerTable(ctor func() Collector) {
	f.partitionFuncs = append(f.partitionFuncs, ctor)

}

// Init builds the map of table constructors and schemas
func (f *TableFactory) Init() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
	}()

	errs := make([]error, 0)

	for _, ctor := range f.partitionFuncs {
		// create an instance of the table to get the identifier
		partition := ctor()

		// register the partition func with the table factory
		f.partitionFuncMap[partition.Identifier()] = ctor
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (f *TableFactory) GetPartition(req *types.CollectRequest) (Collector, error) {
	// get the registered partition constructor for the table
	ctor, ok := f.partitionFuncMap[req.PartitionData.Table]
	if !ok {
		// this type is not registered
		return nil, fmt.Errorf("table not found: %s", req.PartitionData.Table)
	}

	// create the partition
	partition := ctor()

	return partition, nil
}

func (f *TableFactory) GetPartitions() map[string]func() Collector {
	return f.partitionFuncMap
}
