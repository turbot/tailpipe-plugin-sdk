package collection_state

import (
	"encoding/json"
	"testing"
	"time"
)

func TestArtifactCollectionState_MigrateFromLegacyState(t *testing.T) {
	tests := []struct {
		name     string
		legacy   *ArtifactCollectionStateLegacy
		expected *ArtifactCollectionState
	}{
		{
			name: "migrate two trunks with different orders",
			legacy: buildArtifactCollectionStateLegacy(map[string]*TimeRangeCollectionStateLegacy{
				"/trunk1": buildTimeRangeCollectionStateLegacy("2023-10-01 00:00:00", "2023-12-01 01:00:00", time.Hour*24, CollectionOrderChronological, "object1", "object2"),
				"/trunk2": buildTimeRangeCollectionStateLegacy("2023-11-01 00:00:00", "2023-11-30 00:00:00", time.Hour*24, CollectionOrderReverse, "object3"),
			}, timeString("2023-12-01 12:00:00")),
			expected: buildArtifactCollectionState(map[string]*TimeRangeCollectionState{
				"/trunk1": buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour*24,
					buildTimeRangeState("2023-10-01 00:00:00", "2023-11-30 01:00:00", time.Hour*24, CollectionOrderChronological, "object1", "object2"),
				),
				"/trunk2": buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour*24,
					buildTimeRangeState("2023-11-30 00:00:00", "2023-11-01 00:00:00", time.Hour*24, CollectionOrderReverse, "object3"),
				),
			}, time.Hour*24),
		},
		{
			name:     "empty trunks",
			legacy:   buildArtifactCollectionStateLegacy(map[string]*TimeRangeCollectionStateLegacy{}, timeString("2023-12-01 12:00:00")),
			expected: buildArtifactCollectionState(map[string]*TimeRangeCollectionState{}, 0),
		},
		{
			name: "nil trunk",
			legacy: buildArtifactCollectionStateLegacy(map[string]*TimeRangeCollectionStateLegacy{
				"/trunk1": nil,
			}, timeString("2023-12-01 12:00:00")),
			expected: buildArtifactCollectionState(map[string]*TimeRangeCollectionState{}, 0),
		},
		{
			name: "trunk with no end objects",
			legacy: buildArtifactCollectionStateLegacy(map[string]*TimeRangeCollectionStateLegacy{
				"/trunk1": buildTimeRangeCollectionStateLegacy("2023-10-01 00:00:00", "2023-12-01 01:00:00", time.Hour*24, CollectionOrderChronological),
			}, timeString("2023-12-01 12:00:00")),
			expected: buildArtifactCollectionState(map[string]*TimeRangeCollectionState{
				"/trunk1": buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour*24,
					buildTimeRangeState("2023-10-01 00:00:00", "2023-11-30 01:00:00", time.Hour*24, CollectionOrderChronological),
				),
			}, time.Hour*24),
		},
		{
			name: "different granularity",
			legacy: buildArtifactCollectionStateLegacy(map[string]*TimeRangeCollectionStateLegacy{
				"/trunk1": buildTimeRangeCollectionStateLegacy("2023-10-01 00:00:00", "2023-12-01 01:00:00", time.Hour*24, CollectionOrderChronological, "object1"),
				"/trunk2": buildTimeRangeCollectionStateLegacy("2023-11-01 00:00:00", "2023-11-30 00:00:00", time.Hour, CollectionOrderReverse, "object2"),
			}, timeString("2023-12-01 12:00:00")),
			expected: buildArtifactCollectionState(map[string]*TimeRangeCollectionState{
				"/trunk1": buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour*24,
					buildTimeRangeState("2023-10-01 00:00:00", "2023-11-30 01:00:00", time.Hour*24, CollectionOrderChronological, "object1"),
				),
				"/trunk2": buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour,
					buildTimeRangeState("2023-11-30 00:00:00", "2023-11-01 00:00:00", time.Hour, CollectionOrderReverse, "object2"),
				),
			}, time.Hour*24),
		},
		{
			name: "reverse order with multiple end objects",
			legacy: buildArtifactCollectionStateLegacy(map[string]*TimeRangeCollectionStateLegacy{
				"/trunk1": buildTimeRangeCollectionStateLegacy("2023-10-01 00:00:00", "2023-12-01 01:00:00", time.Hour*24, CollectionOrderReverse, "object1", "object2"),
			}, timeString("2023-12-01 12:00:00")),
			expected: buildArtifactCollectionState(map[string]*TimeRangeCollectionState{
				"/trunk1": buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour*24,
					buildTimeRangeState("2023-12-01 01:00:00", "2023-10-01 00:00:00", time.Hour*24, CollectionOrderReverse, "object1", "object2"),
				),
			}, time.Hour*24),
		},
		{
			name: "single trunk, single object",
			legacy: buildArtifactCollectionStateLegacy(map[string]*TimeRangeCollectionStateLegacy{
				"/trunk1": buildTimeRangeCollectionStateLegacy("2023-10-01 00:00:00", "2023-10-02 00:00:00", time.Hour*24, CollectionOrderChronological, "object1"),
			}, timeString("2023-12-01 12:00:00")),
			expected: buildArtifactCollectionState(map[string]*TimeRangeCollectionState{
				"/trunk1": buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour*24,
					buildTimeRangeState("2023-10-01 00:00:00", "2023-10-01 00:00:00", time.Hour*24, CollectionOrderChronological, "object1"),
				),
			}, time.Hour*24),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			legacyBytes, err := json.Marshal(tt.legacy)
			if err != nil {
				t.Fatalf("Failed to marshal legacy state: %v", err)
			}

			newState := NewArtifactCollectionState().(*ArtifactCollectionState)
			err = newState.MigrateFromLegacyState(legacyBytes)
			if err != nil {
				t.Fatalf("Failed to migrate legacy state: %v", err)
			}

			if equal, msg := newState.Compare(tt.expected); !equal {
				t.Errorf("state after migration: %s", msg)
			}
		})
	}
}

// buildArtifactCollectionStateLegacy constructs a legacy artifact collection state for tests
func buildArtifactCollectionStateLegacy(trunks map[string]*TimeRangeCollectionStateLegacy, lastModifiedTime time.Time) *ArtifactCollectionStateLegacy {
	return &ArtifactCollectionStateLegacy{
		TrunkStates:      trunks,
		LastModifiedTime: lastModifiedTime,
	}
}

// buildTimeRangeCollectionStateLegacy constructs a legacy time range collection state for tests
func buildTimeRangeCollectionStateLegacy(fromStr, toStr string, granularity time.Duration, order CollectionOrder, endObjects ...string) *TimeRangeCollectionStateLegacy {
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
	return &TimeRangeCollectionStateLegacy{
		FirstEntryTime:  from,
		LastEntryTime:   to,
		EndTime:         to.Add(-granularity),
		EndObjects:      endObjectsMap,
		Granularity:     granularity,
		CollectionOrder: order,
	}
}

func buildArtifactCollectionState(trunks map[string]*TimeRangeCollectionState, granularity time.Duration) *ArtifactCollectionState {
	return &ArtifactCollectionState{
		TrunkStates: trunks,
		granularity: granularity,
	}
}
