package collection_state

import (
	"testing"
	"time"
)

func TestTimeRangeCollectionStateImpl_OnCollected(t *testing.T) {
	type fields struct {
		FirstEntryTime time.Time
		LastEntryTime  time.Time
		EndTime        time.Time
		EndObjects     map[string]struct{}
		Granularity    time.Duration
	}
	type args struct {
		id        string
		timestamp time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &TimeRangeCollectionStateImpl{
				FirstEntryTime: tt.fields.FirstEntryTime,
				LastEntryTime:  tt.fields.LastEntryTime,
				EndTime:        tt.fields.EndTime,
				EndObjects:     tt.fields.EndObjects,
				Granularity:    tt.fields.Granularity,
			}
			if err := s.OnCollected(tt.args.id, tt.args.timestamp); (err != nil) != tt.wantErr {
				t.Errorf("OnCollected() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
