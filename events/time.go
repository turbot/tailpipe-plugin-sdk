package events

import (
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/types"
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

func TimingCollectionToProto(timing types.TimingCollection) []*proto.Timing {
	var protoTimingCollection = make([]*proto.Timing, len(timing))
	for i, t := range timing { //nolint: govet // TODO Timing contains sync.Mutex, find a nice way of handling this
		protoTimingCollection[i] = &proto.Timing{
			Operation:      t.Operation,
			StartTime:      TimeToProto(t.Start),
			EndTime:        TimeToProto(t.End),
			ActiveDuration: DurationToProto(t.ActiveDuration),
		}
	}
	return protoTimingCollection
}

func TimingCollectionFromProto(protoTimingCollection []*proto.Timing) types.TimingCollection {
	var TimingCollection = make(types.TimingCollection, len(protoTimingCollection))
	for i, t := range protoTimingCollection {
		TimingCollection[i] = types.Timing{
			Operation:      t.Operation,
			Start:          ProtoToTime(t.StartTime),
			End:            ProtoToTime(t.EndTime),
			ActiveDuration: ProtoToDuration(t.ActiveDuration),
		}
	}
	return TimingCollection
}
