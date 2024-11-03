package artifact_mapper

import (
	"context"
)

// Mapper is an interface which provides a method for mapping artifact data to a different format
// Mapper provided by the SDK: [CloudwatchMapper]
type Mapper[R any] interface {
	Identifier() string
	// Map converts artifact data to a different format and either return it as rows,
	// or pass it on to the next mapper in the chain
	Map(context.Context, any) ([]R, error)
}

type MapInitialisedModel interface {
	InitialiseFromMap(m map[string]string) error
}
