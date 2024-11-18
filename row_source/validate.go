package row_source

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Validate(t *testing.T, ctor func() RowSource) {
	s := ctor()

	// identifier
	t.Run("TestIdentifier", func(t *testing.T) {
		TestIdentifier(t, s)
	})
}

func TestIdentifier(t *testing.T, c RowSource) {
	id := c.Identifier()
	assert.NotEmpty(t, id)

	// Assert it's a snake case string in lowercase.
	// Rules:
	// - must start with a lowercase letter
	// - can only contain lowercase letters, numbers, and underscores
	// - max 1 underscore character at a time
	// - must not end with an underscore
	assert.Regexp(t, "^[a-z]+(_[a-z0-9]+)*$", id)
}
