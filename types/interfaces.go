package types

import (
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// Config is an interface that all configuration structs must implement - this includes:
// - table config
// - source config
// - connection config
type Config interface {
	Validate() error
	Identifier() string
}

// DynamicTableConfig is an interface that all dynamic table configuration structs must implement
type DynamicTableConfig interface {
	GetSchema() *schema.RowSchema
}

type RowStruct interface {
	Validate() error
	GetCommonFields() schema.CommonFields
}
