package types

import "path/filepath"

type ExtensionLookup map[string]struct{}

func NewExtensionLookup(extensions []string) ExtensionLookup {
	lookup := make(ExtensionLookup)
	for _, ext := range extensions {
		lookup[ext] = struct{}{}
	}
	return lookup
}

func (l ExtensionLookup) IsValid(path string) bool {
	// empty lookup means all extensions are valid
	if len(l) == 0 {
		return true
	}

	_, valid := l[filepath.Ext(path)]
	return valid
}
