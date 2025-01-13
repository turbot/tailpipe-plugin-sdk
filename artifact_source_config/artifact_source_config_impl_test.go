package artifact_source_config

// #TODO reenable once we support filter pushdown https://github.com/turbot/tailpipe/issues/97
//func TestArtifactSourceConfigBase_Validate(t *testing.T) {
//	type fields struct {
//		Remain     hcl.Body
//		FileLayout *string
//		Filters    []string
//		FilterMap  map[string]*filter.SqlFilter
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		wantErr bool
//	}{
//		{
//			name: "Valid filters - single filter",
//			fields: fields{
//				FileLayout: utils.ToPointer("AWSLogs/%{WORD:org}/CloudTrail"),
//				Filters:    []string{"org = 'org1'"},
//			},
//			wantErr: false,
//		},
//		{
//			name: "Valid filters - multiple filters",
//			fields: fields{
//				FileLayout: utils.ToPointer("AWSLogs/%{WORD:org}/CloudTrail/%{WORD:region}/%{NOTSPACE:file_name}.%{WORD:ext}"),
//				Filters:    []string{"org = 'org1'", "region = 'us-east-1'"},
//			},
//			wantErr: false,
//		},
//		{
//			name: "Filter refer to field not in FileLayout",
//			fields: fields{
//				FileLayout: utils.ToPointer("AWSLogs/%{WORD:org}/CloudTrail/%{NOTSPACE:file_name}.%{WORD:ext}"),
//				Filters:    []string{"org = 'org1'", "region = 'us-east-1'"},
//			},
//			wantErr: true,
//		},
//		{
//			name: "Invalid filter - no LHS property",
//			fields: fields{
//				FileLayout: utils.ToPointer("AWSLogs/%{WORD:org}/CloudTrail"),
//				Filters:    []string{"= 'org1'"},
//			},
//			wantErr: true,
//		},
//		{
//			name: "Invalid filter - multiple LHS properties",
//			fields: fields{
//				FileLayout: utils.ToPointer("AWSLogs/%{WORD:org}/CloudTrail"),
//				Filters:    []string{"org = 'org1' AND account = '123'"},
//			},
//			wantErr: true,
//		},
//		{
//			name: "Empty filters",
//			fields: fields{
//				Filters: []string{},
//			},
//			wantErr: false,
//		},
//		{
//			name: "Nil filters",
//			fields: fields{
//				Filters: nil,
//			},
//			wantErr: false,
//		},
//		{
//			name: "Invalid filter syntax",
//			fields: fields{
//				FileLayout: utils.ToPointer("AWSLogs/%{WORD:org}/CloudTrail"),
//				Filters:    []string{"org =="},
//			},
//			wantErr: true,
//		},
//		{
//			name: "Duplicate filters for the same field",
//			fields: fields{
//				FileLayout: utils.ToPointer("AWSLogs/%{WORD:org}/CloudTrail"),
//				Filters:    []string{"org = 'org1'", "org != 'org2'"},
//			},
//			wantErr: false,
//		},
//		{
//			name: "Valid filters with FileLayout",
//			fields: fields{
//				FileLayout: utils.ToPointer("AWSLogs/%{WORD:org}/%{NUMBER:account_id}/CloudTrail"),
//				Filters:    []string{"org = 'org1'", "account_id = '123'"},
//			},
//			wantErr: false,
//		},
//		{
//			name: "Empty FileLayout and Filters",
//			fields: fields{
//				FileLayout: nil,
//				Filters:    []string{},
//			},
//			wantErr: false,
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			b := &ArtifactSourceConfigImpl{
//				Remain:     tt.fields.Remain,
//				FileLayout: tt.fields.FileLayout,
//				Filters:    tt.fields.Filters,
//				FilterMap:  tt.fields.FilterMap,
//			}
//			if err := b.Validate(); (err != nil) != tt.wantErr {
//				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}
