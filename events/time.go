package events

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TimeToProto(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}

func ProtoToTime(ts *timestamppb.Timestamp) time.Time {
	return ts.AsTime()
}

func DurationToProto(d time.Duration) *durationpb.Duration {
	return durationpb.New(d)
}

func ProtoToDuration(d *durationpb.Duration) time.Duration {
	return d.AsDuration()
}
