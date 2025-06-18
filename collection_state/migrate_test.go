package collection_state

import (
	"testing"
	"time"
)

// implement parse.COnfig
type testConfig struct {
}

func (t testConfig) Validate() error {
	return nil
}

func (t testConfig) Identifier() string {
	return "test"
}

func TestTimeRangeSliceCollectionState_migrate(t1 *testing.T) {

	tests := []struct {
		name               string
		source             any
		newCollectionState func() CollectionState[testConfig]
		want               any
	}{
		{
			name: "migrate ReverseOrderCollectionState",
			source: &ReverseOrderCollectionStateLegacy{
				TimeRanges: []*TimeRangeCollectionStateLegacy{
					buildTimeRangeStateLegacy("2023-10-01 00:00:00", "2023-12-01 01:00:00", time.Hour*24, CollectionOrderReverse, "object1", "object2"),
				},
			},
			//newCollectionState: NewTimeRangeSliceCollectionState[testConfig],
			want: buildTimeRangeSliceState(CollectionOrderReverse, time.Hour*24,
				buildTimeRangeState("2023-10-01 00:00:00", "2023-12-01 01:00:00", time.Hour*24, CollectionOrderReverse, "object1", "object2"),
			),
		},
	}

	var foo CollectionState[testConfig]
	foo = NewTimeRangeSliceCollectionState[testConfig](&timeRange{
		from: time.Time{},
		to:   time.Time{},
	},
		CollectionOrderReverse)
	foo.SetGranularity(time.Hour * 24)

	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			// seralise the source state to JSON
			//sourceJSON, err := json.Marshal(tt.source)
			//if err != nil {
			//	t1.Fatalf("failed to marshal source state: %v", err)
			//}
			//
		})
	}
}

func buildTimeRangeStateLegacy(fromStr, toStr string, granularity time.Duration, order CollectionOrder, endObjects ...string) *TimeRangeCollectionStateLegacy {
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
		EndTime:         to.Add(granularity * -1),
		EndObjects:      endObjectsMap,
		Granularity:     granularity,
		CollectionOrder: order,
	}
}
