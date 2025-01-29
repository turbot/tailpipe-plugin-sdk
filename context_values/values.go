package context_values

import (
	"context"
	"fmt"
	"github.com/turbot/pipe-fittings/v2/contexthelpers"
)

var (
	contextKeyExecutionId = contexthelpers.ContextKey("execution_id")
)

// WithExecutionId adds the execution id to the context
func WithExecutionId(ctx context.Context, executionId string) context.Context {
	return context.WithValue(ctx, contextKeyExecutionId, executionId)
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
