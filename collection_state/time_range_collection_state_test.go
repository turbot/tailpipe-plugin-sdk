package collection_state

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func Test_timeRangeCollectionState_merge(t *testing.T) {
	tests := []struct {
		name  string
		state *timeRangeCollectionState
		other *timeRangeCollectionState
		want  *timeRangeCollectionState
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

			if equal, msg := timeRangeStateEquals(s, tt.want); !equal {
				t.Error(msg)
			}
		})
	}
}

func Test_timeRangeCollectionState_ShouldCollect(t *testing.T) {
	tests := []struct {
		name      string
		state     *timeRangeCollectionState
		id        string
		timestamp time.Time
		want      bool
	}{
		{
			name:      "forward - within range",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:        "obj1",
			timestamp: parseTime("2025-04-03 00:00:00"),
			want:      false,
		},
		{
			name:      "reverse - within range",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:        "obj1",
			timestamp: parseTime("2025-04-03 00:00:00"),
			want:      false,
		},
		{
			name:      "forward - before range",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:        "obj1",
			timestamp: parseTime("2025-03-30 00:00:00"),
			want:      true,
		},
		{
			name:      "reverse - before range",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:        "obj1",
			timestamp: parseTime("2025-03-30 00:00:00"),
			want:      true,
		},
		{
			name:      "forward - after range",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:        "obj1",
			timestamp: parseTime("2025-04-08 00:00:00"),
			want:      true,
		},
		{
			name:      "reverse - after range",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:        "obj1",
			timestamp: parseTime("2025-04-08 00:00:00"),
			want:      true,
		},
		{
			name:      "forward - object not in end objects at boundary",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
			id:        "obj2",
			timestamp: parseTime("2025-04-07 00:00:00"),
			want:      true,
		},
		{
			name:      "reverse - object not in end objects at boundary",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse, "obj1"),
			id:        "obj2",
			timestamp: parseTime("2025-04-07 00:00:00"),
			want:      true,
		},
		{
			name:      "forward - object in end objects at boundary",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
			id:        "obj1",
			timestamp: parseTime("2025-04-07 00:00:00"),
			want:      false,
		},
		{
			name:      "reverse - object in end objects at boundary",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse, "obj1"),
			id:        "obj1",
			timestamp: parseTime("2025-04-01 00:00:00"),
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
		{
			name:      "forward - hour granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour, CollectionOrderChronological),
			id:        "obj1",
			timestamp: parseTime("2025-04-03 14:00:00"),
			want:      false,
		},
		{
			name:      "reverse - hour granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour, CollectionOrderReverse),
			id:        "obj1",
			timestamp: parseTime("2025-04-03 14:00:00"),
			want:      false,
		},
		{
			name:      "forward - minute granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Minute, CollectionOrderChronological),
			id:        "obj1",
			timestamp: parseTime("2025-04-03 14:30:00"),
			want:      false,
		},
		{
			name:      "reverse - minute granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Minute, CollectionOrderReverse),
			id:        "obj1",
			timestamp: parseTime("2025-04-03 14:30:00"),
			want:      false,
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

func Test_timeRangeCollectionState_Contains(t *testing.T) {
	tests := []struct {
		name      string
		state     *timeRangeCollectionState
		timestamp time.Time
		want      bool
	}{
		{
			name:      "forward - within range",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			timestamp: time.Date(2025, 4, 3, 0, 0, 0, 0, time.UTC),
			want:      true,
		},
		{
			name:      "forward - at start time",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			timestamp: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
			want:      true,
		},
		{
			name:      "forward - at end time",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			timestamp: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			want:      true,
		},
		{
			name:      "forward - before range",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			timestamp: time.Date(2025, 3, 31, 0, 0, 0, 0, time.UTC),
			want:      false,
		},
		{
			name:      "forward - after range",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			timestamp: time.Date(2025, 4, 8, 0, 0, 0, 0, time.UTC),
			want:      false,
		},
		{
			name:      "reverse - within range",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			timestamp: time.Date(2025, 4, 3, 0, 0, 0, 0, time.UTC),
			want:      true,
		},
		{
			name:      "reverse - at start time",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			timestamp: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
			want:      true,
		},
		{
			name:      "reverse - at end time",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			timestamp: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			want:      true,
		},
		{
			name:      "reverse - before range",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			timestamp: time.Date(2025, 3, 31, 0, 0, 0, 0, time.UTC),
			want:      false,
		},
		{
			name:      "reverse - after range",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			timestamp: time.Date(2025, 4, 8, 0, 0, 0, 0, time.UTC),
			want:      false,
		},
		{
			name:      "forward - with hour granularity",
			state:     buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 15:00:00", time.Hour, CollectionOrderChronological),
			timestamp: time.Date(2025, 4, 1, 13, 30, 0, 0, time.UTC),
			want:      true,
		},
		{
			name:      "forward - with minute granularity",
			state:     buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:35:00", time.Minute, CollectionOrderChronological),
			timestamp: time.Date(2025, 4, 1, 12, 32, 30, 0, time.UTC),
			want:      true,
		},
		{
			name:      "forward - with second granularity",
			state:     buildTimeRangeState("2025-04-01 12:30:45", "2025-04-01 12:30:55", time.Second, CollectionOrderChronological),
			timestamp: time.Date(2025, 4, 1, 12, 30, 50, 0, time.UTC),
			want:      true,
		},
		{
			name:      "forward - zero granularity",
			state:     buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 0, CollectionOrderChronological),
			timestamp: time.Date(2025, 4, 3, 0, 0, 0, 0, time.UTC),
			want:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.state.Contains(tt.timestamp); got != tt.want {
				t.Errorf("Contains() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_timeRangeCollectionState_IsEmpty(t *testing.T) {
	tests := []struct {
		name  string
		state *timeRangeCollectionState
		want  bool
	}{
		{
			name:  "empty state - zero times",
			state: &timeRangeCollectionState{},
			want:  true,
		},
		{
			name:  "empty state - zero From time",
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

func Test_timeRangeCollectionState_OnCollected(t *testing.T) {
	tests := []struct {
		name          string
		startState    *timeRangeCollectionState
		id            string
		timestamp     time.Time
		wantErr       bool
		expectedState *timeRangeCollectionState
	}{
		{
			name:          "forward - collect within range (between From and To)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     time.Date(2025, 4, 3, 0, 0, 0, 0, time.UTC),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
		{
			name:          "forward - collect at From boundary (exactly at start time)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
		{
			name:          "forward - collect at To boundary (exactly at end time)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
		},
		{
			name:          "forward - collect before From time (outside range)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     time.Date(2025, 3, 31, 0, 0, 0, 0, time.UTC),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
		{
			name:          "forward - collect after To time (extends range)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     time.Date(2025, 4, 8, 0, 0, 0, 0, time.UTC),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-08 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
		},
		{
			name:          "reverse - collect within range (between From and To)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:            "obj1",
			timestamp:     time.Date(2025, 4, 3, 0, 0, 0, 0, time.UTC),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
		},
		{
			name:          "reverse - collect at To boundary (exactly at end time)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:            "obj1",
			timestamp:     time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
		},
		{
			name:          "reverse - collect at From boundary (exactly at start time)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:            "obj1",
			timestamp:     time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse, "obj1"),
		},
		{
			name:          "reverse - collect after To time (outside range)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:            "obj1",
			timestamp:     time.Date(2025, 4, 8, 0, 0, 0, 0, time.UTC),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
		},
		{
			name:          "reverse - collect before From time (extends range)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			id:            "obj1",
			timestamp:     time.Date(2025, 3, 31, 0, 0, 0, 0, time.UTC),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-03-31 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse, "obj1"),
		},
		{
			name:          "forward - collect with hour granularity (within range, non-zero hours)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     time.Date(2025, 4, 1, 13, 30, 0, 0, time.UTC),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
		{
			name:          "forward - collect with minute granularity (within range, non-zero minutes)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     time.Date(2025, 4, 1, 12, 32, 30, 0, time.UTC),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
		{
			name:          "forward - collect with second granularity (within range, non-zero seconds)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "obj1",
			timestamp:     time.Date(2025, 4, 1, 12, 30, 50, 0, time.UTC),
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
			timestamp:     time.Date(2025, 4, 3, 0, 0, 0, 0, time.UTC),
			wantErr:       false,
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
		},
		{
			name:          "forward - collect with empty id (within range, empty identifier)",
			startState:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			id:            "",
			timestamp:     time.Date(2025, 4, 3, 0, 0, 0, 0, time.UTC),
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
			if equal, msg := timeRangeStateEquals(s, tt.expectedState); !equal {
				t.Errorf("state after OnCollected: %s", msg)
			}
		})
	}
}

func Test_timeRangeCollectionState_insideLowerBoundary(t *testing.T) {
	type fields struct {
		From            time.Time
		To              time.Time
		EndObjects      map[string]struct{}
		Granularity     time.Duration
		CollectionOrder CollectionOrder
	}
	type args struct {
		timestamp time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "forward - after start time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 2, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "forward - at start time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "forward - within start time granularity",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 1, 12, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "reverse - before end time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 6, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "reverse - at end time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "reverse - within end time granularity",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 6, 12, 0, 0, 0, time.UTC),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &timeRangeCollectionState{
				From:            tt.fields.From,
				To:              tt.fields.To,
				EndObjects:      tt.fields.EndObjects,
				Granularity:     tt.fields.Granularity,
				CollectionOrder: tt.fields.CollectionOrder,
			}
			if got := s.insideLowerBoundary(tt.args.timestamp); got != tt.want {
				t.Errorf("insideLowerBoundary() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_timeRangeCollectionState_insideUpperBoundary(t *testing.T) {
	type fields struct {
		From            time.Time
		To              time.Time
		EndObjects      map[string]struct{}
		Granularity     time.Duration
		CollectionOrder CollectionOrder
	}
	type args struct {
		timestamp time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "forward - before end time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 6, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "forward - at end time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "forward - within end time granularity",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 6, 12, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "reverse - after start time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 2, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "reverse - at start time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "reverse - within start time granularity",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 1, 12, 0, 0, 0, time.UTC),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &timeRangeCollectionState{
				From:            tt.fields.From,
				To:              tt.fields.To,
				EndObjects:      tt.fields.EndObjects,
				Granularity:     tt.fields.Granularity,
				CollectionOrder: tt.fields.CollectionOrder,
			}
			if got := s.insideUpperBoundary(tt.args.timestamp); got != tt.want {
				t.Errorf("insideUpperBoundary() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_timeRangeCollectionState_outsideLowerBoundary(t *testing.T) {
	type fields struct {
		From            time.Time
		To              time.Time
		EndObjects      map[string]struct{}
		Granularity     time.Duration
		CollectionOrder CollectionOrder
	}
	type args struct {
		timestamp time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "forward - before start time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 3, 31, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "forward - at start time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "forward - within start time granularity",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 1, 12, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "reverse - after end time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 8, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "reverse - at end time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "reverse - within end time granularity",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 6, 12, 0, 0, 0, time.UTC),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &timeRangeCollectionState{
				From:            tt.fields.From,
				To:              tt.fields.To,
				EndObjects:      tt.fields.EndObjects,
				Granularity:     tt.fields.Granularity,
				CollectionOrder: tt.fields.CollectionOrder,
			}
			if got := s.outsideLowerBoundary(tt.args.timestamp); got != tt.want {
				t.Errorf("outsideLowerBoundary() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_timeRangeCollectionState_outsideUpperBoundary(t *testing.T) {
	type fields struct {
		From            time.Time
		To              time.Time
		EndObjects      map[string]struct{}
		Granularity     time.Duration
		CollectionOrder CollectionOrder
	}
	type args struct {
		timestamp time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "forward - after end time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 8, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "forward - at end time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "forward - within end time granularity",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 6, 12, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "reverse - before start time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 3, 31, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "reverse - at start time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "reverse - within start time granularity",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 1, 12, 0, 0, 0, time.UTC),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &timeRangeCollectionState{
				From:            tt.fields.From,
				To:              tt.fields.To,
				EndObjects:      tt.fields.EndObjects,
				Granularity:     tt.fields.Granularity,
				CollectionOrder: tt.fields.CollectionOrder,
			}
			if got := s.outsideUpperBoundary(tt.args.timestamp); got != tt.want {
				t.Errorf("outsideUpperBoundary() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_timeRangeCollectionState_lowerBoundaryTime(t *testing.T) {
	type fields struct {
		From            time.Time
		To              time.Time
		EndObjects      map[string]struct{}
		Granularity     time.Duration
		CollectionOrder CollectionOrder
	}
	type args struct {
		order CollectionOrder
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   time.Time
	}{
		{
			name: "forward - returns From time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				order: CollectionOrderChronological,
			},
			want: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "reverse - returns To time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				order: CollectionOrderReverse,
			},
			want: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &timeRangeCollectionState{
				From:            tt.fields.From,
				To:              tt.fields.To,
				EndObjects:      tt.fields.EndObjects,
				Granularity:     tt.fields.Granularity,
				CollectionOrder: tt.fields.CollectionOrder,
			}
			if got := s.lowerBoundaryTime(tt.args.order); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("lowerBoundaryTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_timeRangeCollectionState_upperBoundaryTime(t *testing.T) {
	type fields struct {
		From            time.Time
		To              time.Time
		EndObjects      map[string]struct{}
		Granularity     time.Duration
		CollectionOrder CollectionOrder
	}
	type args struct {
		order CollectionOrder
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   time.Time
	}{
		{
			name: "forward - returns To time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				order: CollectionOrderChronological,
			},
			want: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "reverse - returns From time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				EndObjects:      map[string]struct{}{},
				Granularity:     24 * time.Hour,
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				order: CollectionOrderReverse,
			},
			want: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &timeRangeCollectionState{
				From:            tt.fields.From,
				To:              tt.fields.To,
				EndObjects:      tt.fields.EndObjects,
				Granularity:     tt.fields.Granularity,
				CollectionOrder: tt.fields.CollectionOrder,
			}
			if got := s.upperBoundaryTime(tt.args.order); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("upperBoundaryTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_timeRangeCollectionState_setUpperBoundaryTime(t *testing.T) {
	type fields struct {
		From            time.Time
		To              time.Time
		EndObjects      map[string]struct{}
		Granularity     time.Duration
		CollectionOrder CollectionOrder
	}
	type args struct {
		newEndTime time.Time
	}
	tests := []struct {
		name          string
		startState    *timeRangeCollectionState
		args          args
		expectedState *timeRangeCollectionState
	}{
		{
			name:       "forward - set new end time after current end",
			startState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			args: args{
				newEndTime: parseTime("2025-04-10 00:00:00"),
			},
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
		{
			name:       "forward - set new end time before current end - do nothing",
			startState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
			args: args{
				newEndTime: parseTime("2025-04-05 00:00:00"),
			},
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
		{
			name:       "forward - set new end time with existing end objects",
			startState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderChronological, "obj1", "obj2"),
			args: args{
				newEndTime: parseTime("2025-04-10 00:00:00"),
			},
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-10 00:00:00", 24*time.Hour, CollectionOrderChronological),
		},
		{
			name:       "reverse - set new end time after current end - do nothing",
			startState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			args: args{
				newEndTime: parseTime("2025-04-10 00:00:00"),
			},
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
		},
		{
			name:       "reverse - set new end time before current end but after start - do nothing",
			startState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
			args: args{
				newEndTime: parseTime("2025-04-05 00:00:00"),
			},
			expectedState: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", 24*time.Hour, CollectionOrderReverse),
		},
		{
			name:       "forward - set new end time with hour granularity",
			startState: buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 15:00:00", time.Hour, CollectionOrderChronological),
			args: args{
				newEndTime: parseTime("2025-04-01 17:00:00"),
			},
			expectedState: buildTimeRangeState("2025-04-01 12:00:00", "2025-04-01 17:00:00", time.Hour, CollectionOrderChronological),
		},
		{
			name:       "forward - set new end time with minute granularity",
			startState: buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:35:00", time.Minute, CollectionOrderChronological),
			args: args{
				newEndTime: parseTime("2025-04-01 12:40:00"),
			},
			expectedState: buildTimeRangeState("2025-04-01 12:30:00", "2025-04-01 12:40:00", time.Minute, CollectionOrderChronological),
		},
		{
			name:       "forward - set new end time with second granularity",
			startState: buildTimeRangeState("2025-04-01 12:30:45", "2025-04-01 12:30:55", time.Second, CollectionOrderChronological),
			args: args{
				newEndTime: parseTime("2025-04-01 12:31:00"),
			},
			expectedState: buildTimeRangeState("2025-04-01 12:30:45", "2025-04-01 12:31:00", time.Second, CollectionOrderChronological),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.startState
			s.setUpperBoundaryTime(tt.args.newEndTime)
			if equal, msg := timeRangeStateEquals(s, tt.expectedState); !equal {
				t.Errorf("state after setUpperBoundaryTime: %s", msg)
			}
		})
	}
}

func buildTimeRangeState(fromStr, toStr string, granularity time.Duration, order CollectionOrder, endObjects ...string) *timeRangeCollectionState {
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
		CollectionOrder: order,
	}
}

func parseTime(timeStr string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05", timeStr)
	if err != nil {
		panic(err)
	}
	return t
}

func timeRangeStateEquals(actual, expected *timeRangeCollectionState) (bool, string) {
	if !actual.From.Equal(expected.From) {
		return false, fmt.Sprintf("From = %v, want %v", actual.From, expected.From)
	}
	if !actual.To.Equal(expected.To) {
		return false, fmt.Sprintf("To = %v, want %v", actual.To, expected.To)
	}
	if len(actual.EndObjects) != len(expected.EndObjects) {
		return false, fmt.Sprintf("EndObjects length = %v, want %v", len(actual.EndObjects), len(expected.EndObjects))
	}
	for k := range expected.EndObjects {
		if _, ok := actual.EndObjects[k]; !ok {
			return false, fmt.Sprintf("EndObjects missing key %v", k)
		}
	}
	if actual.Granularity != expected.Granularity {
		return false, fmt.Sprintf("Granularity = %v, want %v", actual.Granularity, expected.Granularity)
	}
	if actual.CollectionOrder != expected.CollectionOrder {
		return false, fmt.Sprintf("CollectionOrder = %v, want %v", actual.CollectionOrder, expected.CollectionOrder)
	}
	return true, ""
}
