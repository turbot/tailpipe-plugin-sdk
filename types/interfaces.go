package types

import (
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type RowStruct interface {
	Validate() error
	GetCommonFields() schema.CommonFields
}
