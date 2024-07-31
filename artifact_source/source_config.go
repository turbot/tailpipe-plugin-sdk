package artifact_source

// SourceConfigBase is the base configuration for all [Source] configs - it should be embedded in them
type SourceConfigBase struct {
	// location to write temp files
	TmpDir string
}
