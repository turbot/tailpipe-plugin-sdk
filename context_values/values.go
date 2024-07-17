package context_values

import (
	"context"
	"fmt"
	"github.com/turbot/pipe-fittings/contexthelpers"
	"github.com/turbot/tailpipe-plugin-sdk/continuation"
)

var (
	// destPath
	contextKeyDestPath         = contexthelpers.ContextKey("dest_path")
	contextKeyExecutionId      = contexthelpers.ContextKey("execution_id")
	contextKeyContinuationData = contexthelpers.ContextKey("continuation_data")
)

// destp
func DestPathFromContext(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", fmt.Errorf("context is nil")
	}
	val, ok := ctx.Value(contextKeyDestPath).(string)
	if !ok {
		return "", fmt.Errorf("no dest path in context")
	}
	return val, nil
}

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

func ContinuationDataFromContext(ctx context.Context) (continuation.Data, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	val, ok := ctx.Value(contextKeyContinuationData).(continuation.Data)
	if !ok {
		return nil, nil
	}
	return val, nil
}
