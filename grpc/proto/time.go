package proto

import (
	"github.com/turbot/tailpipe-plugin-sdk/types"
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

func TimingMapToProto(timing types.TimingMap) map[string]*Timing {
	var protoTimingMap = make(map[string]*Timing, len(timing))
	for k, v := range timing {
		protoTimingMap[k] = &Timing{
			StartTime: TimeToProto(v.Start),
			EndTime:   TimeToProto(v.End),
			Duration:  DurationToProto(v.Duration()),
		}
	}
	return protoTimingMap
}

func TimingMapFromProto(protoTimingMap map[string]*Timing) types.TimingMap {
	var timingMap = make(types.TimingMap, len(protoTimingMap))
	for k, v := range protoTimingMap {
		timingMap[k] = types.Timing{
			Start: ProtoToTime(v.StartTime),
			End:   ProtoToTime(v.EndTime),
		}
	}
	return timingMap
}
