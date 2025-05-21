package collection_state

import (
	"reflect"
	"testing"
	"time"
)

func TestTimeRangeSliceCollectionState_GetEndTime(t1 *testing.T) {
	type fields struct {
		TimeRanges  []*TimeRangeCollectionStateImpl
		Granularity time.Duration
		Order       CollectionOrder
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Time
	}{
		{
			name: "empty_ranges",
			fields: fields{
				TimeRanges:  []*TimeRangeCollectionStateImpl{},
				Granularity: time.Hour,
				Order:       CollectionOrder(1),
			},
			want: time.Time{},
		},
		{
			name: "single_range",
			fields: fields{
				TimeRanges: []*TimeRangeCollectionStateImpl{
					{
						StartTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
						EndTime:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
					},
				},
				Granularity: time.Hour,
				Order:       CollectionOrder(1),
			},
			want: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			if got := t.GetEndTime(); !reflect.DeepEqual(got, tt.want) {
				t1.Errorf("GetEndTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeSliceCollectionState_GetGranularity(t1 *testing.T) {
	type fields struct {
		TimeRanges  []*TimeRangeCollectionStateImpl
		Granularity time.Duration
		Order       CollectionOrder
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Duration
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			if got := t.GetGranularity(); got != tt.want {
				t1.Errorf("GetGranularity() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeSliceCollectionState_GetStartTime(t1 *testing.T) {
	type fields struct {
		TimeRanges  []*TimeRangeCollectionStateImpl
		Granularity time.Duration
		Order       CollectionOrder
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Time
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			if got := t.GetStartTime(); !reflect.DeepEqual(got, tt.want) {
				t1.Errorf("GetStartTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeSliceCollectionState_IsEmpty(t1 *testing.T) {
	type fields struct {
		TimeRanges  []*TimeRangeCollectionStateImpl
		Granularity time.Duration
		Order       CollectionOrder
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			if got := t.IsEmpty(); got != tt.want {
				t1.Errorf("IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeSliceCollectionState_SetEndTime(t1 *testing.T) {
	type fields struct {
		TimeRanges  []*TimeRangeCollectionStateImpl
		Granularity time.Duration
		Order       CollectionOrder
	}
	type args struct {
		endTime time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			t.SetEndTime(tt.args.endTime)
		})
	}
}

func TestTimeRangeSliceCollectionState_SetGranularity(t1 *testing.T) {
	type fields struct {
		TimeRanges  []*TimeRangeCollectionStateImpl
		Granularity time.Duration
		Order       CollectionOrder
	}
	type args struct {
		granularity time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			t.SetGranularity(tt.args.granularity)
		})
	}
}

func TestTimeRangeSliceCollectionState_addRange(t1 *testing.T) {
	type fields struct {
		TimeRanges  []*TimeRangeCollectionStateImpl
		Granularity time.Duration
		Order       CollectionOrder
	}
	type args struct {
		timestamp time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			if got := t.addRange(tt.args.timestamp); got != tt.want {
				t1.Errorf("addRange() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeSliceCollectionState_mergeRangeWithNext(t1 *testing.T) {
	type fields struct {
		TimeRanges  []*TimeRangeCollectionStateImpl
		Granularity time.Duration
		Order       CollectionOrder
	}
	type args struct {
		idx int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			t.mergeRangeWithNext(tt.args.idx)
		})
	}
}

func TestTimeRangeSliceCollectionState_rangeForTime(t1 *testing.T) {
	type fields struct {
		TimeRanges  []*TimeRangeCollectionStateImpl
		Granularity time.Duration
		Order       CollectionOrder
	}
	type args struct {
		timestamp time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			if got := t.rangeForTime(tt.args.timestamp); got != tt.want {
				t1.Errorf("rangeForTime() = %v, want %v", got, tt.want)
			}
		})
	}
}
