package plugin

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/turbot/tailpipe-plugin-sdk/collection"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"testing"
)

func Validate(t *testing.T, ctor func() (TailpipePlugin, error)) {
	p, err := ctor()
	assert.Nil(t, err)

	t.Run("TestInit", func(t *testing.T) {
		TestInit(t, p)
	})
	t.Run("TestIdentifier", func(t *testing.T) {
		TestIdentifier(t, p)
	})
	t.Run("TestGetSchema", func(t *testing.T) {
		TestGetSchema(t, p)
	})
	t.Run("TestCollections", func(t *testing.T) {
		TestCollections(t, p)
	})
	t.Run("TestSources", func(t *testing.T) {
		TestSources(t, p)
	})

}

func TestInit(t *testing.T, p TailpipePlugin) {
	err := p.Init(context.Background())
	assert.Nil(t, err)

	// ensure base is initialized
	assert.True(t, p.Base().initialized())
}

func TestIdentifier(t *testing.T, p TailpipePlugin) {
	id := p.Identifier()
	assert.NotEmpty(t, id)

	// Assert it's a snake case string in lowercase.
	// Rules:
	// - must start with a lowercase letter
	// - can only contain lowercase letters, numbers, and underscores
	// - max 1 underscore character at a time
	// - must not end with an underscore
	assert.Regexp(t, "^[a-z]+(_[a-z0-9]+)*$", id)
}

func TestGetSchema(t *testing.T, p TailpipePlugin) {
	configSchema := p.GetSchema()
	assert.NotNil(t, configSchema)
}
func TestCollections(t *testing.T, p TailpipePlugin) {
	collections := collection.Factory.GetCollections()

	// plugin must provide at least 1 collection
	assert.True(t, len(collections) > 0)

	for _, c := range collections {
		t.Run("TestInit", func(t *testing.T) {
			collection.Validate(t, c)
		})
	}
}

func TestSources(t *testing.T, p TailpipePlugin) {
	sources := row_source.Factory.GetSources()

	for _, s := range sources {
		t.Run("TestInit", func(t *testing.T) {
			row_source.Validate(t, s)
		})
	}
}
