package artifact_source

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/config_data"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"path/filepath"
	"testing"
)

type testObserver struct {
	Artifacts []string
}

func (t *testObserver) Notify(ctx context.Context, e events.Event) error {
	switch ty := e.(type) {
	case *events.ArtifactDiscovered:
		t.Artifacts = append(t.Artifacts, ty.Info.Name)
	}
	return nil
}

func TestFileSystemSource_DiscoverArtifacts(t *testing.T) {
	type fields struct {
		config []byte
	}

	tests := []struct {
		name   string
		fields fields

		expectedArtifacts []string
		wantErr           bool
	}{
		{
			name: "No filter, pattern matches directory",
			fields: fields{
				config: []byte(`paths = ["./test_data/discover_test_1"]
file_layout = "AWSLogs/%%{WORD:org}/%%{WORD:account_id}/CloudTrail/%%{NOTSPACE:file_name}.%%{WORD:ext}"`),
			},
			expectedArtifacts: []string{
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_3.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_3.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_3.log",
			},
		},
		{
			name: "pattern doe not match - file below pattern leaf",
			fields: fields{
				config: []byte(`paths = ["./test_data/discover_test_1"]
file_layout = "AWSLogs/%%{WORD:org}/%%{WORD:account_id}/CloudTrail/%%{WORD:region}/%%{NOTSPACE:file_name}.%%{WORD:ext}"`),
			},
			expectedArtifacts: []string{},
		},
		{
			name: "no files",
			fields: fields{
				config: []byte(`paths = ["./test_data/discover_test_empty"]
file_layout = "AWSLogs/%%{WORD:org}/%%{WORD:account_id}/CloudTrail/%%{WORD:region}/%%{NOTSPACE:file_name}.%%{WORD:ext}"`),
			},
			expectedArtifacts: []string{},
		},
		{
			name: "org1",
			fields: fields{
				config: []byte(`paths = ["./test_data/discover_test_1"]
file_layout = "AWSLogs/%%{WORD:org}/%%{WORD:account_id}/CloudTrail/%%{NOTSPACE:file_name}.%%{WORD:ext}"
filters = ["org = 'org1'"]`),
			},
			expectedArtifacts: []string{
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_2.log",
			},
		},
		{
			name: "org4 - empty",
			fields: fields{
				config: []byte(`paths = ["./test_data/discover_test_1"]
file_layout = "AWSLogs/%%{WORD:org}/%%{WORD:account_id}/CloudTrail/%%{NOTSPACE:file_name}.%%{WORD:ext}"
filters = ["org = 'org4'"]`),
			},
			expectedArtifacts: []string{},
		},
		{
			name: "org5 - does not exist",
			fields: fields{
				config: []byte(`paths = ["./test_data/discover_test_1"]
file_layout = "AWSLogs/%%{WORD:org}/%%{WORD:account_id}/CloudTrail/%%{NOTSPACE:file_name}.%%{WORD:ext}"
filters = ["org = 'org5'"]`),
			},
			expectedArtifacts: []string{},
		},
		{
			name: "invalid filter",
			fields: fields{
				config: []byte(`paths = ["./test_data/discover_test_1"]
file_layout = "AWSLogs/%%{WORD:org}/%%{WORD:account_id}/CloudTrail/%%{NOTSPACE:file_name}.%%{WORD:ext}"
filters = ["user = 'user1'"]`),
			},
			expectedArtifacts: []string{},
			wantErr:           true,
		},
		{
			name: "org1, account_id 2",
			fields: fields{
				config: []byte(`paths = ["./test_data/discover_test_1"]
file_layout = "AWSLogs/%%{WORD:org}/%%{WORD:account_id}/CloudTrail/%%{NOTSPACE:file_name}.%%{WORD:ext}"
filters = ["org = 'org1'", "account_id = '2'"]`),
			},
			expectedArtifacts: []string{
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_2.log",
			},
		},
		{
			name: "in(org1,org2)",
			fields: fields{
				config: []byte(`paths = ["./test_data/discover_test_1"]
file_layout = "AWSLogs/%%{WORD:org}/%%{WORD:account_id}/CloudTrail/%%{NOTSPACE:file_name}.%%{WORD:ext}"
filters = ["org in ('org1','org2')"]`),
			},
			expectedArtifacts: []string{
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_3.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_3.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_3.log",
			},
		},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			ctx := context_values.WithExecutionId(context.Background(), "test")
			s := &FileSystemSource{}
			any(s).(row_source.BaseSource).RegisterSource(s)

			err := s.Init(ctx, &config_data.ConfigDataImpl{Hcl: tt.fields.config, Id: "file_system"}, nil)
			if err != nil {
				if !tt.wantErr {
					t.Fatalf("DiscoverArtifacts() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			var observer testObserver
			_ = s.AddObserver(&observer)
			err = s.DiscoverArtifacts(ctx)
			if err != nil {
				if !tt.wantErr {
					t.Fatalf("DiscoverArtifacts() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			if len(observer.Artifacts) != len(tt.expectedArtifacts) {
				t.Errorf("DiscoverArtifacts() expected %v artifacts, got %v", len(tt.expectedArtifacts), len(observer.Artifacts))
			}
			for i, expected := range tt.expectedArtifacts {
				e, _ := filepath.Rel(".", expected)
				if observer.Artifacts[i] != e {
					t.Errorf("DiscoverArtifacts() expected artifact %v, got %v", expected, observer.Artifacts[i])
				}
			}
		})
	}
}
