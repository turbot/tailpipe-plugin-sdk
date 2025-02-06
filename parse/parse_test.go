package parse

import (
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"reflect"
	"testing"
)

//
//func TestParseConfig(t *testing.T) {
//	hclBytes := []byte(`layout = <<EOT
//%%{TIMESTAMP_ISO8601:time_local} - %%{NUMBER:event_id} - %%{WORD:user} - \[%%{DATA:location}\] "%%{DATA:message}" %%{WORD:severity} "%%{DATA:additional_info}"
//EOT`)
//
//	_, diags := hclsyntax.ParseConfig(hclBytes, "filename", hcl.Pos{Line: 1, Column: 1})
//	slog.Info("diags", "diags", diags)
//
//}

type customFormat struct {
	// the layout of the log line
	// NOTE that as will contain grok patterns, this property is included in constants.GrokConfigProperties
	// meaning and '{' will be auto-escaped in the hcl
	Layout string `hcl:"layout"`

	// grok patterns to add to the grok parser used to parse the layout
	Patterns map[string]string `hcl:"patterns,optional"`

	// the roq schema must at the minimum provide mapping for the tp_timestamp field
	Schema *schema.RowSchema `hcl:"schema,block"`
}

func (c customFormat) Validate() error {
	return nil
}

func (c customFormat) Identifier() string {
	return "custom"
}

func TestParseFormat(t *testing.T) {
	type args struct {
		hclBytes []byte
	}
	type testCase struct {
		name    string
		args    args
		want    customFormat
		wantErr bool
	}
	tests := []testCase{
		{
			name: "Test 1",
			args: args{
				hclBytes: []byte(`layout = "%%{TIMESTAMP_ISO8601:time_local} - %%{NUMBER:event_id} - %%{WORD:user} - \\[%%{DATA:location}\\] \"%%{DATA:message}\" %%{WORD:severity} \"%%{DATA:additional_info}\""`),
			},
			want: customFormat{
				Layout: `%{TIMESTAMP_ISO8601:time_local} - %{NUMBER:event_id} - %{WORD:user} - \[%{DATA:location}\] "%{DATA:message}" %{WORD:severity} "%{DATA:additional_info}"`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configData := &types.FormatConfigData{
				ConfigDataImpl: &types.ConfigDataImpl{
					Hcl: tt.args.hclBytes,
					Id:  "custom",
				},
			}
			got, err := ParseConfig[*customFormat](configData)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseConfig() got = %v, want %v", got, tt.want)
			}
		})
	}
}

//
//func Test_decodeHclBodyWithNestedStructs(t *testing.T) {
//	type args struct {
//		body     hcl.Body
//		evalCtx  *hcl.EvalContext
//		resource any
//	}
//	tests := []struct {
//		name      string
//		args      args
//		wantDiags hcl.Diagnostics
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if gotDiags := decodeHclBodyWithNestedStructs(tt.args.body, tt.args.evalCtx, tt.args.resource); !reflect.DeepEqual(gotDiags, tt.wantDiags) {
//				t.Errorf("decodeHclBodyWithNestedStructs() = %v, want %v", gotDiags, tt.wantDiags)
//			}
//		})
//	}
//}
