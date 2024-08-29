package table

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Validate(t *testing.T, ctor func() Table) {
	c := ctor()

	// identifier
	t.Run("TestIdentifier", func(t *testing.T) {
		TestIdentifier(t, c)
	})
	// get  row schema
	t.Run("TestGetRowSchema", func(t *testing.T) {
		TestGetRowSchema(t, c)
	})
	// get config schema
	t.Run("TestGetConfigSchema", func(t *testing.T) {
		TestGetConfigSchema(t, c)
	})
}

func TestIdentifier(t *testing.T, c Table) {
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

func TestGetRowSchema(t *testing.T, c Table) {
	rowSchema := c.GetRowSchema()
	assert.NotNil(t, rowSchema)
}

func TestGetConfigSchema(t *testing.T, c Table) {
	configSchema := c.GetConfigSchema()
	assert.NotNil(t, configSchema)
}
