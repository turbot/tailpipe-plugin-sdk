package mappers

import (
	"context"
	"reflect"
	"testing"
)

func TestGrokMapper(t *testing.T) {
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
				Size:      "",
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
			name: "Invalid grok pattern",
			args: args{
				input: `127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`,
			},
			want:    &WebLogRow{},
			wantErr: true,
		},
	}

	patterns := make(map[string]string)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper, err := NewGrokMapper[*WebLogRow](`%{IP:ip} - %{USER:user} \[%{HTTPDATE:timestamp}\] "%{DATA:request}" %{NUMBER:status} (%{NUMBER:size}|-)`, patterns)
			if tt.name == "Invalid grok pattern" {
				mapper, err = NewGrokMapper[*WebLogRow](`%{IP:ip} %{USER:user} \[%{HTTPDATE:timestamp}\] "%{DATA:request}" %{NUMBER:status} (%{NUMBER:size}|-)`, patterns)
			}
			if err != nil {
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
