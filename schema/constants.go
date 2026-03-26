package schema

// Mode values are set on the schema config which is provided in a dynamic table config
type Mode string

const (
	// ModeFull means that the schema is fully defined (the default)
	ModeFull Mode = "full"
	// ModePartial means that the schema is dynamic and is partially defined
	ModePartial Mode = "partial"
	// ModeDynamic means that the schema is fully dynamic and will be determined at runtime
	// NOTE: we weill never explicitly specify this mode - as it means there is no defined schema
	ModeDynamic Mode = "dynamic"
)
