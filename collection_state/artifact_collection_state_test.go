package collection_state

// TODO #enable this test

// func generateFilePaths(count int) []string { //nolint:unused // TODO to be used in TestBenchmarkParseFilenameTemplate
// 	//use this template

// 	//AWSLogs/o-z3cf4qoe7m/(?P<index>\d+)/CloudTrail/(?P<region>[a-z\-]+)/(?P<year>\d{4})/(?P<month>\d{2})/(?P<day>\d{

// 	// AWSLogs/o-z3cf4qoe7m/(?P<index>[^/]+)/CloudTrail/(?P<region>[^/]+)/(?P<year>[^/]+)/(?P<month>[^/]+)/(?P<day>[^/]+)/(?P<index>[^/]+)_CloudTrail_(?P<region>[^/]+)_(?P<date_time>[^/]+)_(?P<random_string>[^/]+).json.gz
// 	// AWSLogs/o-z3cf4qoe7m/{index}/CloudTrail/{region}/{year}/{month}/{day}/{index}_CloudTrail_{region}_{date_time}_{random_string}.json.gz
// 	var res = make([]string, count)
// 	for i := 0; i < count; i++ {
// 		// random date
// 		d := randomDate(time.Date(2020, 1, 0, 0, 0, 0, 0, time.UTC), time.Now())
// 		region := randomRegion()
// 		index := randomIndex(12)
// 		suffix := randomAlphaString(12)

// 		// generate the file path

// 		res[i] = fmt.Sprintf("AWSLogs/o-z3cf4qoe7m/%s/CloudTrail/%s/%d/%d/%d/%s_CloudTrail_%s_%s_%s.json.gz",
// 			index, region, d.Year(), d.Month(), d.Day(), index, region, d.Format("20060102T1504Z"), suffix)
// 	}

// 	return res
// }

// func randomDate(start, end time.Time) time.Time {
// 	// Calculate the difference between the start and end dates
// 	diff := end.Sub(start)

// 	// Generate a random duration within that difference
// 	randomDuration := time.Duration(rand.Int63n(int64(diff)))

// 	// Add the random duration to the start date to get a random date
// 	return start.Add(randomDuration)
// }

// // randomRegion generates a random AWS region from a predefined list
// func randomRegion() string {
// 	regions := []string{
// 		"us-west-1",
// 		"us-west-2",
// 		"us-east-1",
// 		"us-east-2",
// 		"eu-west-1",
// 		"eu-central-1",
// 		"ap-southeast-1",
// 		"ap-northeast-1",
// 		"ap-south-1",
// 		"sa-east-1",
// 	}

// 	// Select a random region from the list
// 	return regions[rand.Intn(len(regions))]
// }

// func randomIndex(length int) string {
// 	// random string
// 	digits := "0123456789"
// 	result := make([]byte, length)

// 	for i := range result {
// 		result[i] = digits[rand.Intn(len(digits))]
// 	}

// 	return string(result)
// }

// func randomAlphaString(length int) string {
// 	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
// 	result := make([]byte, length)

// 	for i := range result {
// 		result[i] = letters[rand.Intn(len(letters))]
// 	}

// 	return string(result)
// }

// func TestBenchmarkParseFilenameTemplate(t *testing.T) {

// 	//template := `AWSLogs/o-z3cf4qoe7m/{index}/CloudTrail/{region}/{year}/{month}/{day}/{index}_CloudTrail_{region}_{date_time}_{random_string}.json.gz`
// 	//fileName := "AWSLogs/o-z3cf4qoe7m/12345/CloudTrail/us-west-2/2024/08/19/12345_CloudTrail_us-west-2_20240819T123456Z_abcdef123456.json.gz"
// 	//res, err := ParseFilenameRegex(fileName, template)
// 	//if err != nil {
// 	//	t.Error(err	)
// 	//}
// 	//fmt.Println(res)

// 	paths := generateFilePaths(100)
// 	filenameTemplate := `AWSLogs/o-z3cf4qoe7m/{index}/CloudTrail/{region}/{year}/{month}/{day}/{index}_CloudTrail_{region}_{date_time}_{random_string}.json.gz`

// 	re, err := convertTemplateToRegex(filenameTemplate)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	start := time.Now()
// 	for _, p := range paths {
// 		parseFilename(p, re)
// 	}
// 	fmt.Println(time.Since(start))
// 	fmt.Println("done")
// }

// func TestParseFilename Template(t *testing.T) {
// 	type args struct {
// 		template string
// 		fileName string
// 	}
// 	tests := []struct {
// 		name string
// 		args args
// 		want map[string]string
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			if got := ParseFilenameTemplate(tt.args.template, tt.args.fileName); !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("ParseFilename() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }
