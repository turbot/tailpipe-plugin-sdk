package row_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"time"
)

type RowSourceParams struct {
	SourceConfigData    *types.SourceConfigData
	ConnectionData      *types.ConnectionConfigData
	CollectionStatePath string
	From                time.Time
	CollectionDir       string
}
