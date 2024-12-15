package collection_state

import (
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/ucarion/urlpath"
	"net/http"
	"net/url"
	"testing"
)

// TODO #testing

//func randomDate(start, end time.Time) time.Time {
//	// Calculate the difference between the start and end dates
//	diff := end.Sub(start)
//
//	// Generate a random duration within that difference
//	randomDuration := time.Duration(rand.Int63n(int64(diff)))
//
//	// Add the random duration to the start date to get a random date
//	return start.Add(randomDuration)
//}
//
//// randomRegion generates a random AWS region from a predefined list
//func randomRegion() string {
//	regions := []string{
//		"us-west-1",
//		"us-west-2",
//		"us-east-1",
//		"us-east-2",
//		"eu-west-1",
//		"eu-central-1",
//		"ap-southeast-1",
//		"ap-northeast-1",
//		"ap-south-1",
//		"sa-east-1",
//	}
//
//	// Select a random region from the list
//	return regions[rand.Intn(len(regions))]
//}
//
//func randomIndex(length int) string {
//	// random string
//	digits := "0123456789"
//	result := make([]byte, length)
//
//	for i := range result {
//		result[i] = digits[rand.Intn(len(digits))]
//	}
//
//	return string(result)
//}
//
//func randomAlphaString(length int) string {
//	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
//	result := make([]byte, length)
//
//	for i := range result {
//		result[i] = letters[rand.Intn(len(letters))]
//	}
//
//	return string(result)
//}
//func generateFilePaths(count int) []string {
//	//use this template
//
//	//AWSLogs/o-z3cf4qoe7m/(?P<index>\d+)/CloudTrail/(?P<region>[a-z\-]+)/(?P<year>\d{4})/(?P<month>\d{2})/(?P<day>\d{
//
//	// AWSLogs/o-z3cf4qoe7m/(?P<index>[^/]+)/CloudTrail/(?P<region>[^/]+)/(?P<year>[^/]+)/(?P<month>[^/]+)/(?P<day>[^/]+)/(?P<index>[^/]+)_CloudTrail_(?P<region>[^/]+)_(?P<date_time>[^/]+)_(?P<random_string>[^/]+).json.gz
//	// AWSLogs/o-z3cf4qoe7m/{index}/CloudTrail/{region}/{year}/{month}/{day}/{index}_CloudTrail_{region}_{date_time}_{random_string}.json.gz
//	var res = make([]string, count)
//	for i := 0; i < count; i++ {
//		// random date
//		d := randomDate(time.Date(2020, 1, 0, 0, 0, 0, 0, time.UTC), time.Now())
//		region := randomRegion()
//		index := randomIndex(12)
//		suffix := randomAlphaString(12)
//
//		// generate the file path
//
//		res[i] = fmt.Sprintf("AWSLogs/o-z3cf4qoe7m/%s/CloudTrail/%s/%d/%d/%d/%s_CloudTrail_%s_%s_%s.json.gz",
//			index, region, d.Year(), d.Month(), d.Day(), index, region, d.Format("20060102T1504Z"), suffix)
//	}
//
//	return res
//}

func TestBenchmarkParseFilenameTemplate(t *testing.T) {
	//
	////template := `AWSLogs/o-z3cf4qoe7m/{index}/CloudTrail/{region}/{year}/{month}/{day}/{index}_CloudTrail_{region}_{date_time}_{random_string}.json.gz`
	////fileName := "AWSLogs/o-z3cf4qoe7m/12345/CloudTrail/us-west-2/2024/08/19/12345_CloudTrail_us-west-2_20240819T123456Z_abcdef123456.json.gz"
	////res, err := ParseFilenameRegex(fileName, template)
	////if err != nil {
	////	t.Error(err	)
	////}
	////fmt.Println(res)
	//
	//paths := generateFilePaths(100)
	//filenameTemplate := `AWSLogs/o-z3cf4qoe7m/{index}/CloudTrail/{region}/{year}/{month}/{day}/{index}_CloudTrail_{region}_{date_time}_{random_string}.json.gz`
	//
	//re, err := convertTemplateToRegex(filenameTemplate)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//start := time.Now()
	//for _, p := range paths {
	//	parseFilename(p, re)
	//}
	//fmt.Println(time.Since(start))
	//fmt.Println("done")
}

//
//func TestParseFilenameTemplate(t *testing.T) {
//	type args struct {
//		template string
//		fileName string
//	}
//	tests := []struct {
//		name string
//		args args
//		want map[string]string
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if got := ParseFilenameTemplate(tt.args.template, tt.args.fileName); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("ParseFilename() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}

func extractPathVars(pattern, path string) (map[string]string, bool) {
	// Define the pattern
	upath := urlpath.New(pattern)

	// Match the path against the pattern
	match, ok := upath.Match(path)
	if !ok {
		return nil, false
	}

	// Extract variables as a map
	return match.Params, true
}

func TestExtractPathVars(t *testing.T) {
	tests := []struct {
		name        string
		pattern     string
		path        string
		expectMatch bool
		expected    map[string]string
	}{
		{
			name:        "Valid path with account_id and region",
			pattern:     "/logs/:account_id/:region/",
			path:        "/logs/12345/us-west-1/",
			expectMatch: true,
			expected: map[string]string{
				"account_id": "12345",
				"region":     "us-west-1",
			},
		},
		{
			name:        "No match due to extra segment",
			pattern:     "/logs/:account_id/:region/",
			path:        "/logs/12345/us-west-1/extra/",
			expectMatch: false,
			expected:    nil,
		},
		{
			name:        "No match due to missing trailing slash",
			pattern:     "/logs/:account_id/:region/",
			path:        "/logs/12345/us-west-1",
			expectMatch: false,
			expected:    nil,
		},
		{
			name:        "Valid path with different account_id and region",
			pattern:     "/logs/:account_id/:region/",
			path:        "/logs/67890/eu-central-1/",
			expectMatch: true,
			expected: map[string]string{
				"account_id": "67890",
				"region":     "eu-central-1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars, matched := extractPathVars(tt.pattern, tt.path)

			if matched != tt.expectMatch {
				t.Errorf("expected match: %v, got: %v", tt.expectMatch, matched)
			}
			if matched && vars != nil {
				for key, value := range tt.expected {
					if vars[key] != value {
						t.Errorf("expected %s: %s, got: %s", key, value, vars[key])
					}
				}
			}
		})
	}
}

// Helper function to extract variables from a path using mux
func extractPathVarsMux(pattern, path string) (map[string]string, bool) {
	router := mux.NewRouter()
	route := router.NewRoute().Path(pattern)

	// Simulate a request using only the URL path
	req := &http.Request{URL: &url.URL{Path: path}}
	routeMatch := mux.RouteMatch{}

	// Attempt to match the route
	if route.Match(req, &routeMatch) {
		return routeMatch.Vars, true
	}
	return nil, false
}

// Test suite for extractPathVars
func TestExtractPathVarsMux(t *testing.T) {
	tests := []struct {
		name        string
		pattern     string
		path        string
		expectMatch bool
		expected    map[string]string
	}{
		{
			name:        "Valid path with account_id and region",
			pattern:     "/logs/{account_id}/{region}/",
			path:        "/logs/12345/us-west-1/",
			expectMatch: true,
			expected: map[string]string{
				"account_id": "12345",
				"region":     "us-west-1",
			},
		},
		{
			name:        "No match due to extra segment",
			pattern:     "/logs/{account_id}/{region}/",
			path:        "/logs/12345/us-west-1/extra/",
			expectMatch: false,
			expected:    nil,
		},
		{
			name:        "No match due to missing trailing slash",
			pattern:     "/logs/{account_id}/{region}/",
			path:        "/logs/12345/us-west-1",
			expectMatch: false,
			expected:    nil,
		},
		{
			name:        "Valid path with different account_id and region",
			pattern:     "/logs/{account_id}/foo{region}/",
			path:        "/logs/67890/fooeu-central-1/",
			expectMatch: true,
			expected: map[string]string{
				"account_id": "67890",
				"region":     "eu-central-1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars, matched := extractPathVarsMux(tt.pattern, tt.path)

			assert.Equal(t, tt.expectMatch, matched, "Match result should match expectation")
			assert.Equal(t, tt.expected, vars, "Extracted variables should match expected values")
		})
	}
}
