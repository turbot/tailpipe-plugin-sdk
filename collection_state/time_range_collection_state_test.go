package collection_state

import (
	"fmt"
	"testing"
	"time"
)

func Test_timeRangeCollectionState_merge(t *testing.T) {
	type args struct {
	}
	tests := []struct {
		name  string
		state *timeRangeCollectionState
		other *timeRangeCollectionState
		want  *timeRangeCollectionState
	}{
		{
			name:  "merge overlapping ranges",
			state: buildState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, "obj1"),
			other: buildState("2025-04-05 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, "obj2"),
			want:  buildState("2025-04-01 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, "obj2"),
		},
		{
			name:  "no merge - other range starts after our end",
			state: buildState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, "obj1"),
			other: buildState("2025-04-08 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, "obj2"),
			want:  buildState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, "obj1"),
		},
		{
			name:  "no merge - other range ends before our end",
			state: buildState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, "obj1"),
			other: buildState("2025-04-02 00:00:00", "2025-04-05 00:00:00", 24*time.Hour, "obj2"),
			want:  buildState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, "obj1"),
		},
		{
			name:  "merge with nil other",
			state: buildState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, "obj1"),
			other: nil,
			want:  buildState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, "obj1"),
		},
		{
			name:  "no merge - our range contains other range",
			state: buildState("2025-04-01 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, "obj1"),
			other: buildState("2025-04-03 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, "obj2"),
			want:  buildState("2025-04-01 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, "obj1"),
		},
		{
			name:  "merge - other range contains our range",
			state: buildState("2025-04-03 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, "obj1"),
			other: buildState("2025-04-01 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, "obj2"),
			want:  buildState("2025-04-03 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, "obj2"),
		},
		{
			name:  "merge with second granularity",
			state: buildState("2025-04-01 12:30:45", "2025-04-01 12:30:55", time.Second, "obj1"),
			other: buildState("2025-04-01 12:30:50", "2025-04-01 12:31:00", time.Second, "obj2"),
			want:  buildState("2025-04-01 12:30:45", "2025-04-01 12:31:00", time.Second, "obj2"),
		},
		{
			name:  "merge with minute granularity",
			state: buildState("2025-04-01 12:30:00", "2025-04-01 12:35:00", time.Minute, "obj1"),
			other: buildState("2025-04-01 12:33:00", "2025-04-01 12:40:00", time.Minute, "obj2"),
			want:  buildState("2025-04-01 12:30:00", "2025-04-01 12:40:00", time.Minute, "obj2"),
		},
		{
			name:  "merge with hour granularity",
			state: buildState("2025-04-01 12:00:00", "2025-04-01 15:00:00", time.Hour, "obj1"),
			other: buildState("2025-04-01 14:00:00", "2025-04-01 17:00:00", time.Hour, "obj2"),
			want:  buildState("2025-04-01 12:00:00", "2025-04-01 17:00:00", time.Hour, "obj2"),
		},
		{
			name:  "merge with day granularity",
			state: buildState("2025-04-01 00:00:00", "2025-04-05 00:00:00", 24*time.Hour, "obj1"),
			other: buildState("2025-04-03 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, "obj2"),
			want:  buildState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, "obj2"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.state
			s.merge(tt.other)

			if equal, msg := stateEquals(s, tt.want, 0); !equal {
				t.Error(msg)
			}
		})
	}
}

func buildState(fromStr, toStr string, granularity time.Duration, endObjects ...string) *timeRangeCollectionState {
	from, err := time.Parse("2006-01-02 15:04:05", fromStr)
	if err != nil {
		panic(err)
	}
	to, err := time.Parse("2006-01-02 15:04:05", toStr)
	if err != nil {
		panic(err)
	}
	endObjectsMap := make(map[string]struct{})
	for _, obj := range endObjects {
		endObjectsMap[obj] = struct{}{}
	}
	return &timeRangeCollectionState{
		From:            from,
		To:              to,
		EndObjects:      endObjectsMap,
		Granularity:     granularity,
		CollectionOrder: CollectionOrderChronological,
	}
}

func stateEquals(actual, expected *timeRangeCollectionState, index int) (bool, string) {
	if !actual.From.Equal(expected.From) {
		return false, fmt.Sprintf("range[%v].From = %v, want %v", index, actual.From, expected.From)
	}
	if !actual.To.Equal(expected.To) {
		return false, fmt.Sprintf("range[%v].To = %v, want %v", index, actual.To, expected.To)
	}
	if len(actual.EndObjects) != len(expected.EndObjects) {
		return false, fmt.Sprintf("range[%v].EndObjects length = %v, want %v", index, len(actual.EndObjects), len(expected.EndObjects))
	}
	for k := range expected.EndObjects {
		if _, ok := actual.EndObjects[k]; !ok {
			return false, fmt.Sprintf("range[%v].EndObjects missing key %v", index, k)
		}
	}
	if actual.Granularity != expected.Granularity {
		return false, fmt.Sprintf("range[%v].Granularity = %v, want %v", index, actual.Granularity, expected.Granularity)
	}
	if actual.CollectionOrder != expected.CollectionOrder {
		return false, fmt.Sprintf("range[%v].CollectionOrder = %v, want %v", index, actual.CollectionOrder, expected.CollectionOrder)
	}
	return true, ""
}
