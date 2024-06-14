package event

import (
	"github.com/turbot/tailpipe-plugin-sdk/collection"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/source"
)

type Row struct {
	Artifact source.Artifact
	Row      collection.Row
}

// ToProto implements plugin.Event
func (r Row) ToProto() (*proto.Event, error) {
	row, err := proto.RowToProto(r.Row)
	if err != nil {
		return nil, err

	}
	e := &proto.Event{
		Type: proto.EventType_ROW_EVENT,
		Event: &proto.Event_RowEvent{
			RowEvent: &proto.EventRow{
				Artifact: r.Artifact.ToProto(),
				Row:      row,
			},
		},
	}
	return e, nil
}

//
//func RowFromProto(p *proto.EventRow) *Row {
//	return &Row{
//		Artifact: source.ArtifactFromProto(p.GetArtifact()), // Assuming ArtifactFromProto is implemented
//		// TODO
//		//Row:      collection.RowFromProto(p.GetRow()),    // Assuming RowFromProto is implemented
//	}
//}

//
//type ExtractRowsStart struct {
//	Artifact *Artifact
//}
//
//type ExtractRowsEnd struct {
//	Artifact *Artifact
//	Error    error
//}
//
//type SyncArtifactStart struct {
//	Artifact *Artifact
//}
//
//type SyncArtifactEnd struct {
//	Artifact *Artifact
//	Error    error
//}
