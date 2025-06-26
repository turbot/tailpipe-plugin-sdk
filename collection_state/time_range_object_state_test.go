package collection_state

import (
	"testing"
	"time"
)

func Test_timeRangeObjectState_merge(t *testing.T) {
	tests := []struct {
		name  string
		state *TimeRangeObjectState
		other *TimeRangeObjectState
		want  *TimeRangeObjectState
	}{
		{
			name:  "merge overlapping ranges",
			state: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
			other: buildTimeRangeState("2025-04-05 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
			want:  buildTimeRangeState("2025-04-01 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
		},
		{
			name:  "other range starts after our end",
			state: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
			other: buildTimeRangeState("2025-04-08 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
			want:  buildTimeRangeState("2025-04-01 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
		},
		{
			name:  "no merge - other range ends before our end",
			state: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
			other: buildTimeRangeState("2025-04-02 00:00:00", "2025-04-05 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
			want:  buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
		},
		{
			name:  "merge with nil other",
			state: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
			other: nil,
			want:  buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
		},
		{
			name:  "no merge - our range contains other range",
			state: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
			other: buildTimeRangeState("2025-04-03 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
			want:  buildTimeRangeState("2025-04-01 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
		},
		{
			name:  "merge - other range contains our range",
			state: buildTimeRangeState("2025-04-03 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
			other: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
			want:  buildTimeRangeState("2025-04-01 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
		},
		{
			name:  "merge with second granularity",
			state: buildTimeRangeState("2025-04-01 12:30:45", "2025-04-01 12:30:55", time.Second, CollectionOrderChronological, "obj1"),
			other: buildTimeRangeState("2025-04-01 12:30:50", "2025-04-01 12:31:00", time.Second, CollectionOrderChronological, "obj2"),
			want:  buildTimeRangeState("2025-04-01 12:30:45", "2025-04-01 12:31:00", time.Second, CollectionOrderChronological, "obj2"),
		},
		{
			name:  "merge with minute granularity",
			state: buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:35:00", time.Minute, CollectionOrderChronological, "obj1"),
			other: buildTimeRangeState("2025-04-01 12:33:00", "2025-04-01 12:40:00", time.Minute, CollectionOrderChronological, "obj2"),
			want:  buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:40:00", time.Minute, CollectionOrderChronological, "obj2"),
		},
		{
			name:  "merge with hour granularity",
			state: buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 15:00:00", time.Hour, CollectionOrderChronological, "obj1"),
			other: buildTimeRangeState("2025-04-01 14:00:00", "2025-04-01 17:00:00", time.Hour, CollectionOrderChronological, "obj2"),
			want:  buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 17:00:00", time.Hour, CollectionOrderChronological, "obj2"),
		},
		{
			name:  "merge with day granularity",
			state: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-05 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
			other: buildTimeRangeState("2025-04-03 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
			want:  buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
		},
		{
			name:  "merge non-contiguous ranges",
			state: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-03 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
			other: buildTimeRangeState("2025-04-05 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
			want:  buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.state
			s.merge(tt.other)

			if equal, msg := s.Compare(tt.want); !equal {
				t.Error(msg)
			}
		})
	}
}

func Test_timeRangeObjectState_ShouldCollect(t *testing.T) {
	tests := []struct {
		name      string
		state     *TimeRangeObjectState
		id        string
		timestamp time.Time
		want      bool
	}{
		{
			name:      "forward - within range - day granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:        "obj1",
			timestamp: timeString("2025-04-03 00:00:00"),
			want:      false,
		},
		{
			name:      "forward - within range - hour granularity",
			state:     buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 15:00:00", time.Hour, CollectionOrderChronological),
			id:        "obj1",
			timestamp: timeString("2025-04-01 13:30:00"),
			want:      false,
		},
		{
			name:      "forward - within range - minute granularity",
			state:     buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:35:00", time.Minute, CollectionOrderChronological),
			id:        "obj1",
			timestamp: timeString("2025-04-01 12:32:30"),
			want:      false,
		},
		{
			name:      "reverse - within range - day granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:        "obj1",
			timestamp: timeString("2025-04-03 00:00:00"),
			want:      false,
		},
		{
			name:      "reverse - within range - hour granularity",
			state:     buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 15:00:00", time.Hour, CollectionOrderReverse),
			id:        "obj1",
			timestamp: timeString("2025-04-01 13:30:00"),
			want:      false,
		},
		{
			name:      "reverse - within range - minute granularity",
			state:     buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:35:00", time.Minute, CollectionOrderReverse),
			id:        "obj1",
			timestamp: timeString("2025-04-01 12:32:30"),
			want:      false,
		},
		{
			name:      "forward - before from time - day granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:        "obj1",
			timestamp: timeString("2025-03-30 00:00:00"),
			want:      true,
		},
		{
			name:      "forward - before from time - hour granularity",
			state:     buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 15:00:00", time.Hour, CollectionOrderChronological),
			id:        "obj1",
			timestamp: timeString("2025-04-01 11:30:00"),
			want:      true,
		},
		{
			name:      "forward - before from time - minute granularity",
			state:     buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:35:00", time.Minute, CollectionOrderChronological),
			id:        "obj1",
			timestamp: timeString("2025-04-01 12:29:30"),
			want:      true,
		},
		{
			name:      "reverse - before from time - day granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:        "obj1",
			timestamp: timeString("2025-03-30 00:00:00"),
			want:      true,
		},
		{
			name:      "reverse - before from time - hour granularity",
			state:     buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 15:00:00", time.Hour, CollectionOrderReverse),
			id:        "obj1",
			timestamp: timeString("2025-04-01 11:30:00"),
			want:      true,
		},
		{
			name:      "reverse - before from time - minute granularity",
			state:     buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:35:00", time.Minute, CollectionOrderReverse),
			id:        "obj1",
			timestamp: timeString("2025-04-01 12:29:30"),
			want:      true,
		},
		{
			name:      "forward - after To time - day granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:        "obj1",
			timestamp: timeString("2025-04-08 00:00:00"),
			want:      true,
		},
		{
			name:      "forward - after To time - hour granularity",
			state:     buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 15:00:00", time.Hour, CollectionOrderChronological),
			id:        "obj1",
			timestamp: timeString("2025-04-01 15:30:00"),
			want:      true,
		},
		{
			name:      "forward - after To time - minute granularity",
			state:     buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:35:00", time.Minute, CollectionOrderChronological),
			id:        "obj1",
			timestamp: timeString("2025-04-01 12:35:30"),
			want:      true,
		},
		{
			name:      "reverse - after To time - day granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:        "obj1",
			timestamp: timeString("2025-04-08 00:00:00"),
			want:      true,
		},
		{
			name:      "reverse - after To time - hour granularity",
			state:     buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 15:00:00", time.Hour, CollectionOrderReverse),
			id:        "obj1",
			timestamp: timeString("2025-04-01 15:30:00"),
			want:      true,
		},
		{
			name:      "reverse - after To time - minute granularity",
			state:     buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:35:00", time.Minute, CollectionOrderReverse),
			id:        "obj1",
			timestamp: timeString("2025-04-01 12:35:30"),
			want:      true,
		},
		{
			name:      "forward - at To time - object not in end objects - day granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
			id:        "obj2",
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      true,
		},
		{
			name:      "forward - at To time - object not in end objects - hour granularity",
			state:     buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 15:00:00", time.Hour, CollectionOrderChronological, "obj1"),
			id:        "obj2",
			timestamp: timeString("2025-04-01 15:00:00"),
			want:      true,
		},
		{
			name:      "forward - at To time - object not in end objects - minute granularity",
			state:     buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:35:00", time.Minute, CollectionOrderChronological, "obj1"),
			id:        "obj2",
			timestamp: timeString("2025-04-01 12:35:00"),
			want:      true,
		},
		{
			name:      "reverse - at from time - object not in end objects - day granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse, "obj1"),
			id:        "obj2",
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      true,
		},
		{
			name:      "reverse - at from time - object not in end objects - hour granularity",
			state:     buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 15:00:00", time.Hour, CollectionOrderReverse, "obj1"),
			id:        "obj2",
			timestamp: timeString("2025-04-01 12:00:00"),
			want:      true,
		},
		{
			name:      "reverse - at from time - object not in end objects - minute granularity",
			state:     buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:35:00", time.Minute, CollectionOrderReverse, "obj1"),
			id:        "obj2",
			timestamp: timeString("2025-04-01 12:30:00"),
			want:      true,
		},
		{
			name:      "forward - at To time - object in end objects - day granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
			id:        "obj1",
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      false,
		},
		{
			name:      "forward - at To time - object in end objects - hour granularity",
			state:     buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 15:00:00", time.Hour, CollectionOrderChronological, "obj1"),
			id:        "obj1",
			timestamp: timeString("2025-04-01 15:00:00"),
			want:      false,
		},
		{
			name:      "forward - at To time - object in end objects - minute granularity",
			state:     buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:35:00", time.Minute, CollectionOrderChronological, "obj1"),
			id:        "obj1",
			timestamp: timeString("2025-04-01 12:35:00"),
			want:      false,
		},
		{
			name:      "reverse - at from time - object in end objects - day granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse, "obj1"),
			id:        "obj1",
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      false,
		},
		{
			name:      "reverse - at from time - object in end objects - hour granularity",
			state:     buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 15:00:00", time.Hour, CollectionOrderReverse, "obj1"),
			id:        "obj1",
			timestamp: timeString("2025-04-01 12:00:00"),
			want:      false,
		},
		{
			name:      "reverse - at from time - object in end objects - minute granularity",
			state:     buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:35:00", time.Minute, CollectionOrderReverse, "obj1"),
			id:        "obj1",
			timestamp: timeString("2025-04-01 12:30:00"),
			want:      false,
		},
		{
			name:      "forward - zero granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 0, CollectionOrderChronological, "obj1"),
			id:        "obj2",
			timestamp: time.Time{},
			want:      true,
		},
		{
			name:      "reverse - zero granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 0, CollectionOrderReverse, "obj1"),
			id:        "obj2",
			timestamp: time.Time{},
			want:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.state.ShouldCollect(tt.id, tt.timestamp); got != tt.want {
				t.Errorf("ShouldCollect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_timeRangeObjectState_IsEmpty(t *testing.T) {
	tests := []struct {
		name  string
		state *TimeRangeObjectState
		want  bool
	}{
		{
			name:  "empty state - zero times",
			state: &TimeRangeObjectState{},
			want:  true,
		},
		{
			name:  "empty state - zero from time",
			state: buildTimeRangeState("0001-01-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			want:  true,
		},
		{
			name:  "empty state - zero To time",
			state: buildTimeRangeState("2025-04-01 00:00:00", "0001-01-01 00:00:00", 24*time.Hour, CollectionOrderChronological),
			want:  true,
		},
		{
			name:  "non-empty state - forward order",
			state: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			want:  false,
		},
		{
			name:  "non-empty state - reverse order",
			state: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			want:  false,
		},
		{
			name:  "non-empty state - with end objects",
			state: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1", "obj2"),
			want:  false,
		},
		{
			name:  "non-empty state - hour granularity",
			state: buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 15:00:00", time.Hour, CollectionOrderChronological),
			want:  false,
		},
		{
			name:  "non-empty state - minute granularity",
			state: buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:35:00", time.Minute, CollectionOrderChronological),
			want:  false,
		},
		{
			name:  "non-empty state - second granularity",
			state: buildTimeRangeState("2025-04-01 12:30:45", "2025-04-01 12:30:55", time.Second, CollectionOrderChronological),
			want:  false,
		},
		{
			name:  "non-empty state - zero granularity",
			state: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 0, CollectionOrderChronological),
			want:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.state.IsEmpty(); got != tt.want {
				t.Errorf("IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_timeRangeObjectState_OnCollected(t *testing.T) {
	tests := []struct {
		name          string
		startState    *TimeRangeObjectState
		id            string
		timestamp     time.Time
		wantErr       bool
		expectedState *TimeRangeObjectState
	}{
		{
			name:          "forward - collect within range (between from and To)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     timeString("2025-04-03 00:00:00"),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
		{
			name:          "forward - collect at from boundary (exactly at start time)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     timeString("2025-04-01 00:00:00"),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
		{
			name:          "forward - collect at To boundary (exactly at end time)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     timeString("2025-04-07 00:00:00"),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
		},
		{
			name:          "forward - collect before from time (outside range)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     timeString("2025-03-31 00:00:00"),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
		{
			name:          "forward - collect after To time (extends range)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     timeString("2025-04-08 00:00:00"),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-08 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
		},
		{
			name:          "reverse - collect within range (between from and To)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:            "obj1",
			timestamp:     timeString("2025-04-03 00:00:00"),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
		},
		{
			name:          "reverse - collect at To boundary (exactly at end time)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:            "obj1",
			timestamp:     timeString("2025-04-07 00:00:00"),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
		},
		{
			name:          "reverse - collect at from boundary (exactly at start time)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:            "obj1",
			timestamp:     timeString("2025-04-01 00:00:00"),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse, "obj1"),
		},
		{
			name:          "reverse - collect after To time (outside range)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:            "obj1",
			timestamp:     timeString("2025-04-08 00:00:00"),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
		},
		{
			name:          "reverse - collect before from time (extends range)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:            "obj1",
			timestamp:     timeString("2025-03-31 00:00:00"),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-03-31 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse, "obj1"),
		},
		{
			name:          "forward - collect with hour granularity (within range, non-zero hours)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     timeString("2025-04-01 13:30:00"),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
		{
			name:          "forward - collect with minute granularity (within range, non-zero minutes)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     timeString("2025-04-01 12:32:30"),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
		{
			name:          "forward - collect with second granularity (within range, non-zero seconds)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     timeString("2025-04-01 12:30:50"),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
		{
			name:          "forward - collect with zero time (within range, zero time value)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     time.Time{}, // Use zero time for zero granularity
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
		{
			name:          "forward - collect with existing end objects (within range, preserves existing objects)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
			id:            "obj1",
			timestamp:     timeString("2025-04-03 00:00:00"),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
		},
		{
			name:          "forward - collect with empty id (within range, empty identifier)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "",
			timestamp:     timeString("2025-04-03 00:00:00"),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.startState
			err := s.OnCollected(tt.id, tt.timestamp)
			if (err != nil) != tt.wantErr {
				t.Errorf("OnCollected() error = %v, wantErr %v", err, tt.wantErr)
			}
			if equal, msg := s.Compare(tt.expectedState); !equal {
				t.Errorf("state after OnCollected: %s", msg)
			}
		})
	}
}

func buildTimeRangeState(fromStr, toStr string, granularity time.Duration, order CollectionOrder, endObjects ...string) *TimeRangeObjectState {
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
	return &TimeRangeObjectState{
		TimeRange: CollectionTimeRange{
			From:            from,
			To:              to,
			CollectionOrder: order,
		},
		EndObjects:  endObjectsMap,
		Granularity: granularity,
	}
}

func timeString(timeStr string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05", timeStr)
	if err != nil {
		panic(err)
	}
	return t
}
