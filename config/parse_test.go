package config

//
//func TestParseConfig(t *testing.T) {
//	type args struct {
//		configString []byte
//		filename     string
//		startPos     hcl.Pos
//		configStruct any
//	}
//	tests := []struct {
//		name    string
//		args    args
//		want    any
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, err := ParseConfig(tt.args.configString, tt.args.filename, tt.args.startPos, tt.args.configStruct)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("ParseConfig() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("ParseConfig() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
