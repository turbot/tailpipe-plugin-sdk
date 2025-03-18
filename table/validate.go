package table

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Validate(t *testing.T, ctor func() Collector) {
	c := ctor()

	// identifier
	t.Run("TestIdentifier", func(t *testing.T) {
		TestIdentifier(t, c)
	})
	// get  row schema
	t.Run("TestSchema", func(t *testing.T) {
		TestSchema(t, c)
	})
}

func TestIdentifier(t *testing.T, c Collector) {
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

func TestSchema(t *testing.T, c Collector) {
	rowSchema, err := c.GetSchema()
	assert.Nil(t, err)
	assert.NotNil(t, rowSchema)
	err = rowSchema.Validate()
	assert.Nil(t, err)
}
