package readers

import (
	"context"
	"fmt"
	"io"
	"os"
)

func init() {
	RegisterReader("", newLocalReader)
}

func newLocalReader(_ context.Context, _ string, path string) (func() error, io.ReadSeekCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return func() error { return nil }, nil, fmt.Errorf("failed to open local file: %w", err)
	}
	return f.Close, f, nil
}
