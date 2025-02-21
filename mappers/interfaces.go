package mappers

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// MapInitialisedRow is an interface which provides a means to initialise a row struct from a string map
// this is used in combination with the GonxMapper/GrokMapper
type MapInitialisedRow interface {
	types.RowStruct
	InitialiseFromMap(m map[string]string, tableSchema *schema.TableSchema) error
}

// Mapper is a generic interface which provides a method for mapping raw source data into row structs
// R is the type of the row struct which the mapperFunc outputs
type Mapper[R types.RowStruct] interface {
	Identifier() string
	// Map converts raw rows to the desired format (type 'R')
	Map(context.Context, any, ...MapOption[R]) (R, error)
}

type CustomTableMapper[R types.RowStruct] interface {
	Mapper[R]
	SetSchema(*schema.TableSchema)
}
