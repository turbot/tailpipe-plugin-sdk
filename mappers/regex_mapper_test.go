package mappers

import (
	"context"
	"reflect"
	"testing"

	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type WebLogRow struct {
	IPAddress string
	User      string
	Timestamp string
	Request   string
	Status    string
	Size      string
}

func (r *WebLogRow) InitialiseFromMap(m map[string]string, tableSchema *schema.TableSchema) error {
	if ip, ok := m["ip"]; ok {
		r.IPAddress = ip
	}
	if user, ok := m["user"]; ok {
		r.User = user
	}
	if timestamp, ok := m["timestamp"]; ok {
		r.Timestamp = timestamp
	}
	if request, ok := m["request"]; ok {
		r.Request = request
	}
	if status, ok := m["status"]; ok {
		r.Status = status
	}
	if size, ok := m["size"]; ok {
		r.Size = size
	}
	return nil
}

func (r *WebLogRow) Validate() error {
	return nil
}

func (r *WebLogRow) GetCommonFields() schema.CommonFields {
	var res schema.CommonFields
	return res
}

func TestRegexMapper(t *testing.T) {
	type args struct {
		input string
	}
	tests := []struct {
		name    string
		args    args
		want    *WebLogRow
		wantErr bool
	}{
		{
			name: "Valid input",
			args: args{
				input: `127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`,
			},
			want: &WebLogRow{
				IPAddress: "127.0.0.1",
				User:      "frank",
				Timestamp: "10/Oct/2000:13:55:36 -0700",
				Request:   "GET /apache_pb.gif HTTP/1.0",
				Status:    "200",
				Size:      "2326",
			},
			wantErr: false,
		},
		{
			name: "Empty input string",
			args: args{
				input: ``,
			},
			want:    &WebLogRow{},
			wantErr: true,
		},
		{
			name: "Missing fields in input",
			args: args{
				input: `127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0"`,
			},
			want:    &WebLogRow{},
			wantErr: true,
		},
		{
			name: "Extra fields in input",
			args: args{
				input: `127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 extra-field`,
			},
			want: &WebLogRow{
				IPAddress: "127.0.0.1",
				User:      "frank",
				Timestamp: "10/Oct/2000:13:55:36 -0700",
				Request:   "GET /apache_pb.gif HTTP/1.0",
				Status:    "200",
				Size:      "2326",
			},
			wantErr: false,
		},
		{
			name: "No user field (anonymous request)",
			args: args{
				input: `127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`,
			},
			want: &WebLogRow{
				IPAddress: "127.0.0.1",
				User:      "-",
				Timestamp: "10/Oct/2000:13:55:36 -0700",
				Request:   "GET /apache_pb.gif HTTP/1.0",
				Status:    "200",
				Size:      "2326",
			},
			wantErr: false,
		},
		{
			name: "No size field (e.g., HTTP HEAD request with no body)",
			args: args{
				input: `127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 -`,
			},
			want: &WebLogRow{
				IPAddress: "127.0.0.1",
				User:      "frank",
				Timestamp: "10/Oct/2000:13:55:36 -0700",
				Request:   "GET /apache_pb.gif HTTP/1.0",
				Status:    "200",
				Size:      "-",
			},
			wantErr: false,
		},
		{
			name: "Malformed timestamp",
			args: args{
				input: `127.0.0.1 - frank [INVALID_TIMESTAMP] "GET /apache_pb.gif HTTP/1.0" 200 2326`,
			},
			want:    &WebLogRow{},
			wantErr: true,
		},
		{
			name: "Long values in fields",
			args: args{
				input: `255.255.255.255 - superlongusername [10/Oct/2000:13:55:36 -0700] "GET /very/long/path/that/keeps/going HTTP/1.0" 200 999999999`,
			},
			want: &WebLogRow{
				IPAddress: "255.255.255.255",
				User:      "superlongusername",
				Timestamp: "10/Oct/2000:13:55:36 -0700",
				Request:   "GET /very/long/path/that/keeps/going HTTP/1.0",
				Status:    "200",
				Size:      "999999999",
			},
			wantErr: false,
		},
		{
			name: "Invalid characters in fields",
			args: args{
				input: `127.0.0.1 - frank$ [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`,
			},
			want:    &WebLogRow{},
			wantErr: true,
		},
		{
			name: "Whitespace variations",
			args: args{
				input: `   127.0.0.1   -   frank   [10/Oct/2000:13:55:36 -0700]   "GET /apache_pb.gif HTTP/1.0"   200   2326   `,
			},
			want:    &WebLogRow{},
			wantErr: true,
		},
		{
			name: "IPv6 address (unsupported case)",
			args: args{
				input: `[2001:db8::ff00:42:8329] - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`,
			},
			want:    &WebLogRow{},
			wantErr: true,
		},
		{
			name: "Input does not match pattern",
			args: args{
				input: `invalid log line`,
			},
			want:    &WebLogRow{},
			wantErr: true,
		},
		{
			name: "Invalid regex pattern",
			args: args{
				input: `127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`,
			},
			want:    &WebLogRow{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper, err := NewRegexMapper[*WebLogRow](`(?P<ip>\d+\.\d+\.\d+\.\d+) - (?P<user>[a-zA-Z0-9_-]+) \[(?P<timestamp>\d{2}/[A-Za-z]+/\d{4}:\d{2}:\d{2}:\d{2} [-+]\d{4})\] "(?P<request>[^"]+)" (?P<status>\d+|-) (?P<size>\d+|-)`)
			if tt.name == "Invalid regex pattern" {
				mapper, err = NewRegexMapper[*WebLogRow](`(?P<ip>\d+\.\d+\.\d+\.\d+) (?P<user>[a-zA-Z0-9_-]+) \[(?P<timestamp>\d{2}/[A-Za-z]+/\d{4}:\d{2}:\d{2}:\d{2} [-+]\d{4})\] "(?P<request>[^"]+)" (?P<status>\d+|-) (?P<size>\d+|-)`)
			}

			if err != nil {
				if tt.wantErr {
					return
				}
				t.Errorf("unexpected error: %v", err)
			}

			got, err := mapper.Map(context.Background(), tt.args.input)
			if err != nil {
				if tt.wantErr {
					return
				}
				t.Errorf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
