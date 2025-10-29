package readers

import (
	"context"
	"fmt"
	"io"
	"os"
)

// ReaderFactory creates a ReadSeekCloser for a given resource path.
type ReaderFactory func(ctx context.Context, bucket, path string) (func() error, io.ReadSeekCloser, error)

var factories = map[string]ReaderFactory{}

// RegisterReader registers a reader factory for a given scheme.
func RegisterReader(scheme string, factory ReaderFactory) {
	factories[scheme] = factory
}

// GetReader returns an io.ReadSeekCloser for a given filename.
func GetReader(ctx context.Context, scheme, bucket, path string) (func() error, io.ReadSeekCloser, error) {
	if factory, ok := factories[scheme]; ok {
		closer, rs, err := factory(ctx, bucket, path)
		return closer, rs, err
	}

	// Fallback — local file
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open local file: %w", err)
	}
	return f.Close, f, nil
}

// WithReader runs a function with a ReadSeeker, automatically closing after use.
func WithReader(ctx context.Context, scheme, bucket, path string, f func(remote bool, rs io.ReadSeeker) error) error {
	closeReader, rs, err := GetReader(ctx, scheme, bucket, path)
	if err != nil {
		return err
	}
	defer func() {
		_ = closeReader()
	}()
	return f(scheme != "", rs)
}
