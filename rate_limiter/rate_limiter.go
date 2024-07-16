package rate_limiter

import (
	"context"
	"fmt"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
	"strings"
)

type APILimiter struct {
	Name string

	// underlying rate limiter
	limiter *rate.Limiter
	// semaphore to control concurrency
	sem            *semaphore.Weighted
	maxConcurrency int64
}

func NewAPILimiter(l *Definition) *APILimiter {
	res := &APILimiter{
		Name:           l.Name,
		maxConcurrency: l.MaxConcurrency,
	}
	if l.FillRate != 0 {
		res.limiter = rate.NewLimiter(l.FillRate, int(l.BucketSize))
	}
	if l.MaxConcurrency != 0 {
		res.sem = semaphore.NewWeighted(l.MaxConcurrency)
	}
	return res
}

func (l *APILimiter) String() string {
	limiterString := ""
	concurrencyString := ""
	if l.limiter != nil {
		limiterString = fmt.Sprintf("Limit(/s): %v, Burst: %d", l.limiter.Limit(), l.limiter.Burst())
	}
	if l.maxConcurrency >= 0 {
		concurrencyString = fmt.Sprintf("MaxConcurrency: %d", l.maxConcurrency)
	}
	return strings.Join([]string{limiterString, concurrencyString}, " ")
}

func (l *APILimiter) acquireSemaphore(ctx context.Context) error {
	if l.sem == nil {
		return nil
	}
	return l.sem.Acquire(ctx, 1)
}

func (l *APILimiter) TryToAcquireSemaphore() bool {
	if l.sem == nil {
		return true
	}
	return l.sem.TryAcquire(1)
}

func (l *APILimiter) Wait(ctx context.Context) error {
	if err := l.acquireSemaphore(ctx); err != nil {
		return err
	}
	if l.limiter != nil {
		return l.limiter.Wait(ctx)
	}
	return nil
}

func (l *APILimiter) Release() {
	if l.sem == nil {
		return
	}
	l.sem.Release(1)

}
