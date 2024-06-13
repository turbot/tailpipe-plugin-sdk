package proto

import (
	"github.com/turbot/tailpipe-plugin-sdk/collection"
	"log/slog"
)

func (x *Row) populateColumnsFields(row collection.Row) error {
	// TODO reflect on row and populate Columns field based on parquey tags
	return nil
}

func RowToProto(row collection.Row) (*Row, error) {
	r := &Row{
		Connection:  row.GetConnection(),
		Year:        int32(row.GetYear()),
		Month:       int32(row.GetMonth()),
		Day:         int32(row.GetDay()),
		TpID:        row.GetTpID(),
		TpTimestamp: row.GetTpTimestamp(),
	}
	// populate the additional field using parquet tags
	if err := r.populateColumnsFields(row); err != nil {
		slog.Warn("RowToProto to failed: error populating columns fields", "error", err)
		return r, err
	}
	return r, nil
}
