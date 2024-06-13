package plugin

//
//func TestIdentifier(t *testing.T, p Plugin) {
//	id := p.Identifier()
//	assert.NotEmpty(t, id)
//	// Assert it's a snake case string in lowercase.
//	// Rules:
//	// - must start with a lowercase letter
//	// - can only contain lowercase letters, numbers, and underscores
//	// - max 1 underscore character at a time
//	// - must not end with an underscore
//	assert.Regexp(t, "^[a-z]+(_[a-z0-9]+)*$", id)
//}
//
//func TestContextIsPreserved(t *testing.T, p Plugin) {
//	type contextKey string
//	ctx := context.WithValue(context.Background(), contextKey("foo"), "bar")
//	assert.NoError(t, p.Init(ctx))
//	assert.Equal(t, ctx, p.Context())
//}
//
//func RunConformanceTests(t *testing.T, plugins ...Plugin) {
//	for _, p := range plugins {
//		t.Run("TestIdentifier", func(t *testing.T) {
//			TestIdentifier(t, p)
//		})
//		t.Run("TestContextIsPreserved", func(t *testing.T) {
//			TestContextIsPreserved(t, p)
//		})
//	}
//}
