package row_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

// ResolvedFromTime is a struct that holds the 'resolved' from time and the source that provided it
// From time is determined from either:
// - the default (T-7d
// - the from time provided in the request
// - the end time od the collection state
type ResolvedFromTime struct {
	Time   time.Time
	Source string
}

func (t ResolvedFromTime) ToProto() *proto.ResolvedFromTime {
	return &proto.ResolvedFromTime{
		FromTime: timestamppb.New(t.Time),
		Source:   t.Source,
	}
}

func ResolvedFromTimeFromProto(proto *proto.ResolvedFromTime) *ResolvedFromTime {
	if proto == nil {
		return nil
	}
	return &ResolvedFromTime{
		Time:   proto.FromTime.AsTime(),
		Source: proto.Source,
	}
}
