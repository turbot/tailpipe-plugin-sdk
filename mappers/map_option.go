package mappers

import (
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type MapOption[R types.RowStruct] func(Mapper[R])
