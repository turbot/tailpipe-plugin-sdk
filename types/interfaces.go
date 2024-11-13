package types

import "github.com/turbot/tailpipe-plugin-sdk/enrichment"

type RowStruct interface {
	Validate() error
	GetCommonFields() enrichment.CommonFields
}
