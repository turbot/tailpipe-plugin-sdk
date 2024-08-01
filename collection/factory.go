package collection

import (
	"context"
	"errors"
	"fmt"

	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// Factory is a global CollectionFactory instance
var Factory = newCollectionFactory()

type CollectionFactory struct {
	// maps of constructors for  the various registsred types
	collections map[string]func() Collection
	// map of collection schemas
	schemaMap schema.SchemaMap
}

func newCollectionFactory() CollectionFactory {
	return CollectionFactory{
		collections: make(map[string]func() Collection),
		schemaMap:   make(schema.SchemaMap),
	}
}

func (f *CollectionFactory) RegisterCollections(collectionFunc ...func() Collection) error {
	errs := make([]error, 0)
	for _, ctor := range collectionFunc {
		// create an instance of the collection to get the identifier
		c := ctor()
		// register the collection
		f.collections[c.Identifier()] = ctor

		// get the schema for the collection row type
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

func (f *CollectionFactory) GetSchema() schema.SchemaMap {
	return f.schemaMap
}

func (f *CollectionFactory) GetCollection(ctx context.Context, req *proto.CollectRequest) (Collection, error) {
	// get the registered constructor for the collection
	ctor, ok := f.collections[req.CollectionData.Type]
	if !ok {
		// this type is not registered
		return nil, fmt.Errorf("collection not found: %s", req.CollectionData.Type)
	}

	// create the collection
	col := ctor()

	//  register the collection implementation with the base struct (_before_ calling Init)
	// create an interface type to use - we do not want to expose this function in the Collection interface
	type BaseCollection interface{ RegisterImpl(Collection) }
	baseCol, ok := col.(BaseCollection)
	if !ok {
		return nil, fmt.Errorf("collection implementation must embed collection.RowSourceBase")
	}
	baseCol.RegisterImpl(col)

	// prepare the data needed for Init

	// convert req into collectionConfigData and sourceConfigData
	collectionConfigData := hcl.DataFromProto(req.CollectionData)
	sourceConfigData := hcl.DataFromProto(req.SourceData)

	err := col.Init(ctx, collectionConfigData, sourceConfigData)
	if err != nil {
		return nil, fmt.Errorf("failed to initialise collection: %w", err)
	}

	return col, nil
}
