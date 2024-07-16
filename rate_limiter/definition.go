package rate_limiter

import (
	"fmt"
	"golang.org/x/time/rate"
	"strings"
)

type Definition struct {
	// the limiter name
	Name string
	// the actual limiter config
	FillRate   rate.Limit
	BucketSize int64
	// the max concurrency supported
	MaxConcurrency int64
}

func (d *Definition) String() string {
	limiterString := ""
	concurrencyString := ""
	if d.FillRate >= 0 {
		limiterString = fmt.Sprintf("Limit(/s): %v, Burst: %d", d.FillRate, d.BucketSize)
	}
	if d.MaxConcurrency >= 0 {
		concurrencyString = fmt.Sprintf("MaxConcurrency: %d", d.MaxConcurrency)
	}
	return strings.Join([]string{limiterString, concurrencyString}, " ")
}

func (d *Definition) Validate() []string {
	var validationErrors []string
	if d.Name == "" {
		validationErrors = append(validationErrors, "rate limiter definition must specify a name")
	}
	if (d.FillRate == 0 || d.BucketSize == 0) && d.MaxConcurrency == 0 {
		validationErrors = append(validationErrors, "rate limiter definition must definer either a rate limit or max concurrency")
	}

	return validationErrors
}
