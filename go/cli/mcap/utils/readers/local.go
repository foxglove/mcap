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

func newLocalReader(_ context.Context, _ string, path string) (io.ReadSeekCloser, func() error, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, func() error { return nil }, fmt.Errorf("failed to open local file: %w", err)
	}
	return f, f.Close, nil
}
