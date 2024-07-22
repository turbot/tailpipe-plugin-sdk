package context_values

import (
	"context"
	"fmt"
	"github.com/turbot/pipe-fittings/contexthelpers"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
)

var (
	// destPath

	contextKeyExecutionId = contexthelpers.ContextKey("execution_id")
	contextKeyPagingData  = contexthelpers.ContextKey("paging_data")
)

// WithExecutionId adds the execution id to the context
func WithExecutionId(ctx context.Context, executionId string) context.Context {
	return context.WithValue(ctx, contextKeyExecutionId, executionId)
}

// WithPagingData adds the paging data to the context
func WithPagingData(ctx context.Context, data paging.Data) context.Context {
	return context.WithValue(ctx, contextKeyPagingData, data)
}

// ExecutionIdFromContext returns the execution id from the context
func ExecutionIdFromContext(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", fmt.Errorf("context is nil")
	}
	val, ok := ctx.Value(contextKeyExecutionId).(string)
	if !ok {
		return "", fmt.Errorf("no execution id in context")
	}
	return val, nil
}

// PagingDataFromContext returns the paging data from the context
func PagingDataFromContext[T paging.Data](ctx context.Context) (T, bool) {
	var empty T
	if ctx == nil {
		return empty, false
	}
	val, ok := ctx.Value(contextKeyPagingData).(T)
	return val, ok
}
