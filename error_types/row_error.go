package error_types

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
	"golang.org/x/exp/maps"

	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type RowOperationType string

const (
	RowOperationTypeMapping    RowOperationType = "mapping"
	RowOperationTypeEnrichment RowOperationType = "enrichment"
	RowOperationTypeValidation RowOperationType = "validation"
)

type RowError interface {
	error
	GetSource() string
	GetOperation() RowOperationType
}

type RowErrorWithMessage struct {
	Source    string
	Operation RowOperationType
	Message   string
}

func (r *RowErrorWithMessage) GetSource() string {
	return r.Source
}

func (r *RowErrorWithMessage) GetOperation() RowOperationType {
	return r.Operation
}

func (r *RowErrorWithMessage) Error() string {
	return r.Message
}

type RowErrorWithFields struct {
	Source        string
	Operation     RowOperationType
	MissingFields []string
	InvalidFields []string
}

func (r *RowErrorWithFields) GetSource() string {
	return r.Source
}

func (r *RowErrorWithFields) GetOperation() RowOperationType {
	return r.Operation
}

func (r *RowErrorWithFields) Error() string {
	switch {
	case len(r.MissingFields) > 0 && len(r.InvalidFields) > 0:
		return fmt.Sprintf("missing/invalid fields: %s, %s", strings.Join(r.MissingFields, ","), strings.Join(r.InvalidFields, ","))
	case len(r.MissingFields) > 0:
		return fmt.Sprintf("missing fields: %s", strings.Join(r.MissingFields, ","))
	case len(r.InvalidFields) > 0:
		return fmt.Sprintf("invalid fields: %s", strings.Join(r.InvalidFields, ","))
	}
	return ""
}

type operationErrorAggregate struct {
	missingFields map[string]struct{}
	invalidFields map[string]struct{}
	messages      map[string]struct{}
	count         int64
}

func (o *operationErrorAggregate) Update(err RowError) {
	var rowError *RowErrorWithMessage
	var rowErrorWithFields *RowErrorWithFields

	switch {
	case errors.As(err, &rowError):
		if o.messages == nil {
			o.messages = make(map[string]struct{})
		}
		o.messages[rowError.Message] = struct{}{}
	case errors.As(err, &rowErrorWithFields):
		if o.missingFields == nil {
			o.missingFields = make(map[string]struct{})
		}

		if o.invalidFields == nil {
			o.invalidFields = make(map[string]struct{})
		}

		for _, field := range rowErrorWithFields.MissingFields {
			o.missingFields[field] = struct{}{}
		}

		for _, field := range rowErrorWithFields.InvalidFields {
			o.invalidFields[field] = struct{}{}
		}
	}

	o.count++
}

// RowErrors is an aggregation of row errors, grouping by RowOperationType and error message
type RowErrors struct {
	// source/operation/error message/count
	errors map[string]map[RowOperationType]operationErrorAggregate
	Total  int64

	mut *sync.RWMutex
}

// NewRowErrors creates a new RowErrors correctly, should always be used to create a new RowErrors
func NewRowErrors() RowErrors {
	return RowErrors{
		errors: make(map[string]map[RowOperationType]operationErrorAggregate),
		Total:  0,
		mut:    &sync.RWMutex{},
	}
}

// Add adds a RowError to the RowErrors
func (r *RowErrors) Add(err RowError) {
	r.mut.Lock()
	defer r.mut.Unlock()

	// increment error count
	r.Total++

	source := err.GetSource()
	operation := err.GetOperation()

	// get or create the map for the source
	if _, ok := r.errors[source]; !ok {
		r.errors[source] = make(map[RowOperationType]operationErrorAggregate)
	}

	// get operationErrorAggregate for the operation
	operationErrors := r.errors[source][operation]

	operationErrors.Update(err)

	r.errors[source][operation] = operationErrors
}

// Errors returns one row per source, with a summary of the errors
// e.g. "source: 100 rows failed mapping with 2 missing fields: field1, field2"
// or "source: 100 rows failed with 2 errors" if there are multiple error messages which are not related to missing/invalid fields
// or "source: 100 rows failed with error: message" if there is only one error message for the source
func (r *RowErrors) Errors() []string {
	r.mut.RLock()
	defer r.mut.RUnlock()

	var results []string
	for source, operationMap := range r.errors {
		var rowCount int64
		messages := make(map[string]struct{})
		fields := make(map[string]struct{})
		fieldErrorTypes := make(map[string]struct{})
		operations := make([]string, 0, len(operationMap))

		for operation, operationErrors := range operationMap {
			operations = append(operations, string(operation))

			rowCount += operationErrors.count

			// de-dupe messages and fields
			for message := range operationErrors.messages {
				messages[message] = struct{}{}
			}

			for field := range operationErrors.missingFields {
				fieldErrorTypes["missing"] = struct{}{}
				fields[field] = struct{}{}
			}

			for field := range operationErrors.invalidFields {
				fieldErrorTypes["invalid"] = struct{}{}
				fields[field] = struct{}{}
			}

		}

		// these are for displaying in error string
		operationsDisplay := strings.Join(operations, "/")
		fieldErrorTypesDisplay := strings.Join(maps.Keys(fieldErrorTypes), "/")
		fieldsDisplay := strings.Join(maps.Keys(fields), ", ")

		// determine the error message to display for the source
		switch {
		case len(fields) > 0:
			results = append(results, fmt.Sprintf("%s: %s %s failed %s with %s fields: %s", source, humanize.Comma(rowCount), utils.Pluralize("row", int(rowCount)), operationsDisplay, fieldErrorTypesDisplay, fieldsDisplay))
		case len(messages) == 1:
			// single error message so display it
			msgText := maps.Keys(messages)[0]
			results = append(results, fmt.Sprintf("%s: %s %s failed %s with error: %s", source, humanize.Comma(rowCount), utils.Pluralize("row", int(rowCount)), operationsDisplay, msgText))
		case len(messages) > 1:
			// multiple error messages so just display the count
			msgCount := len(messages)
			results = append(results, fmt.Sprintf("%s: %s %s failed %s with %d errors", source, humanize.Comma(rowCount), utils.Pluralize("row", int(rowCount)), operationsDisplay, msgCount))
		}
	}

	return results
}

// ToProto converts the RowErrors to a protobuf representation
func (r *RowErrors) ToProto() *proto.RowErrors {
	r.mut.RLock()
	defer r.mut.RUnlock()

	out := &proto.RowErrors{
		Total:  r.Total,
		Errors: make(map[string]*proto.RowErrorsByOperation),
	}

	for source, operationMap := range r.errors {
		operationErrorsMap := make(map[string]*proto.OperationErrorAggregate)
		for operation, operationErrors := range operationMap {
			operationErrorsMap[string(operation)] = &proto.OperationErrorAggregate{
				Count:         operationErrors.count,
				MissingFields: maps.Keys(operationErrors.missingFields),
				InvalidFields: maps.Keys(operationErrors.invalidFields),
				Messages:      maps.Keys(operationErrors.messages),
			}
		}
		out.Errors[source] = &proto.RowErrorsByOperation{
			OperationErrors: operationErrorsMap,
		}
	}

	return out
}

// RowErrorsFromProto converts a protobuf representation to a RowErrors
func RowErrorsFromProto(proto *proto.RowErrors) *RowErrors {
	r := NewRowErrors()

	r.Total = proto.Total
	r.errors = make(map[string]map[RowOperationType]operationErrorAggregate)

	for source, sourceErrors := range proto.Errors {
		operationMap := make(map[RowOperationType]operationErrorAggregate)

		for opName, opAgg := range sourceErrors.OperationErrors {
			operation := RowOperationType(opName)
			operationErrors := operationErrorAggregate{
				missingFields: make(map[string]struct{}),
				invalidFields: make(map[string]struct{}),
				messages:      make(map[string]struct{}),
				count:         opAgg.Count,
			}

			for _, field := range opAgg.MissingFields {
				operationErrors.missingFields[field] = struct{}{}
			}

			for _, field := range opAgg.InvalidFields {
				operationErrors.invalidFields[field] = struct{}{}
			}

			for _, message := range opAgg.Messages {
				operationErrors.messages[message] = struct{}{}
			}

			operationMap[operation] = operationErrors
		}
		r.errors[source] = operationMap
	}

	return &r
}

// EnsureRowError ensures that the error is a RowError, if not it converts it to a RowErrorWithMessage
func EnsureRowError(source string, operation RowOperationType, err error) RowError {
	var rowError RowError
	if !errors.As(err, &rowError) {
		return &RowErrorWithMessage{
			Source:    source,
			Operation: operation,
			Message:   err.Error(),
		}
	}
	return rowError
}
