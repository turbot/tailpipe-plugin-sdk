package artifact

import (
	"bufio"
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"os"
)

// FileRowLoader is an Loader that can loads a file from a path and extracts the contenst a line at a time
type FileRowLoader struct {
}

func NewFileRowLoader() (Loader, error) {
	return &FileRowLoader{}, nil
}

func (g FileRowLoader) Identifier() string {
	return FileRowLoaderIdentifier
}

// Load implements Loader
// Extracts an object from a  file
func (g FileRowLoader) Load(ctx context.Context, info *types.ArtifactInfo) ([]any, error) {
	inputPath := info.Name
	f, err := os.Open(inputPath)
	if err != nil {
		return nil, fmt.Errorf("error opening %s: %w", inputPath, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	var res []any
	for scanner.Scan() {
		// check context cancellation
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// todo stream data / convert to iterator
		// get the line of text and append to the result
		res = append(res, scanner.Text())
	}
	return res, nil
}
