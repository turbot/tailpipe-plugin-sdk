package plugin

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"testing"
)

//TODO #validation validate sources which implement ArtifactRowSource override th enecessray functions

func Validate(t *testing.T, ctor func() (TailpipePlugin, error)) {
	p, err := ctor()
	assert.Nil(t, err)

	t.Run("TestInit", func(t *testing.T) {
		TestInit(t, p)
	})
	t.Run("TestIdentifier", func(t *testing.T) {
		TestIdentifier(t, p)
	})
	t.Run("TestDescribe", func(t *testing.T) {
		TestDescribe(t, p)
	})
	t.Run("TestTables", func(t *testing.T) {
		TestTables(t, p)
	})
	t.Run("TestSources", func(t *testing.T) {
		TestSources(t, p)
	})

}

func TestDescribe(t *testing.T, p TailpipePlugin) {
	schema, err := p.Describe()
	assert.Nil(t, err)
	assert.NotNil(t, schema)
	assert.NotEmpty(t, schema)
}

func TestInit(t *testing.T, p TailpipePlugin) {
	err := p.Init(context.Background())
	assert.Nil(t, err)

	// ensure base is initialized
	assert.True(t, p.Impl().initialized())
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

func TestTables(t *testing.T, p TailpipePlugin) {
	// TODO re-add validation

	//partitions := table.Factory.GetPartitions()
	//
	//// plugin must provide at least 1 collection
	//assert.True(t, len(tables) > 0)
	//
	//for _, c := range tables {
	//	t.Run("TestInit", func(t *testing.T) {
	//		table.Validate(t, c)
	//	})
	//}
}

func TestSources(t *testing.T, p TailpipePlugin) {
	sources := row_source.Factory.GetSources()

	for _, s := range sources {
		t.Run("TestInit", func(t *testing.T) {
			row_source.Validate(t, s)
		})
	}
}
