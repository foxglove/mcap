package readers

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
)

// Automatically register GCS reader on import.
func init() {
	RegisterReader("gs", newGCSReader)
}

// Factory for GCS readers (called from registry).
func newGCSReader(ctx context.Context, bucket, path string) (io.ReadSeekCloser, func() error, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, func() error { return nil }, fmt.Errorf("failed to create GCS client: %w", err)
	}

	object := client.Bucket(bucket).Object(path)
	rs, err := NewGCSReadSeekCloser(ctx, object)
	if err != nil {
		_ = client.Close()
		return nil, func() error { return nil }, fmt.Errorf("failed to create GCS reader: %w", err)
	}

	return rs, client.Close, nil
}

// GCSReadSeekCloser provides seekable read access for GCS objects.
type GCSReadSeekCloser struct {
	ctx    context.Context
	object *storage.ObjectHandle
	reader io.ReadCloser
	size   int64
	offset int64
}

// NewGCSReadSeekCloser creates a new seekable GCS reader.
func NewGCSReadSeekCloser(ctx context.Context, object *storage.ObjectHandle) (*GCSReadSeekCloser, error) {
	r, err := object.NewReader(ctx)
	if err != nil {
		return nil, err
	}

	return &GCSReadSeekCloser{
		ctx:    ctx,
		object: object,
		reader: r,
		size:   r.Attrs.Size,
		offset: 0,
	}, nil
}

// Read reads data into p.
func (r *GCSReadSeekCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.offset += int64(n)
	return n, err
}

// Seek reopens the reader from a new offset.
func (r *GCSReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = r.offset + offset
	case io.SeekEnd:
		target = r.size + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if target == r.offset {
		return target, nil
	}
	if target < 0 || target > r.size {
		return 0, fmt.Errorf("seek out of bounds: %d", target)
	}

	_ = r.reader.Close()
	newReader, err := r.object.NewRangeReader(r.ctx, target, -1)
	if err != nil {
		return 0, err
	}
	r.reader = newReader
	r.offset = target
	return target, nil
}

// Close closes the underlying reader.
func (r *GCSReadSeekCloser) Close() error {
	return r.reader.Close()
}
