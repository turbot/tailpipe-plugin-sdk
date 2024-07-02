package schema

import (
	"reflect"
	"testing"
)

func TestParseParquetTag(t *testing.T) {
	type args struct {
		tag string
	}
	tests := []struct {
		name    string
		args    args
		want    *ParquetTag
		wantErr bool
	}{

		{
			name: "success",
			args: args{
				tag: "name=foo,type=string",
			},
			want: &ParquetTag{
				Name: "foo",
				Type: "string",
			},
			wantErr: false,
		},
		{
			name: "Extra comma",
			args: args{
				tag: "name=foo,type=string,",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Extra comma and space",
			args: args{
				tag: "name=foo,type=string, ",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "additional unrecognized kv pair",
			args: args{
				tag: "name=foo,type=string,another=tag",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "missing type",
			args: args{
				tag: "name=foo",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "missing name",
			args: args{
				tag: "type=string",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "missing name and type",
			args: args{
				tag: "",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "unrecognized tag",
			args: args{
				tag: "foo=bar,type=string",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "duplicate recognized tag",
			args: args{
				tag: "name=foo,type=string,name=bar",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "spaces in tag",
			args: args{
				tag: "name=foo, type=string",
			},
			want: &ParquetTag{
				Name: "foo",
				Type: "string",
			},
			wantErr: false,
		},
		{
			name: "spaces in kv pair",
			args: args{
				tag: "name= foo, type =string",
			},
			want: &ParquetTag{
				Name: "foo",
				Type: "string",
			},
			wantErr: false,
		},
		{
			name: "spaces in kv pair and tag",
			args: args{
				tag: "name= foo, type =string ",
			},
			want: &ParquetTag{
				Name: "foo",
				Type: "string",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseParquetTag(tt.args.tag)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseParquetTag() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseParquetTag() got = %v, want %v", got, tt.want)
			}
		})
	}
}
