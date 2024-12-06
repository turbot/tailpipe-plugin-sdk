package schema

type Mode string

const (
	// ModeStatic means that the schema is fully defined (the default)
	ModeStatic Mode = "static"
	// ModePartial means that the schema is dynamic and is partially defined
	ModePartial Mode = "partial"
	// ModeDynamic means that the schema is fully dynamic and will be determined at runtime
	ModeDynamic Mode = "dynamic"
)
