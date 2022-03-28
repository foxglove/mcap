package utils

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
)

type GCSReadSeekCloser struct {
	size   int64
	object *storage.ObjectHandle
	ctx    context.Context
	offset int64
	r      io.ReadCloser
}

func (r *GCSReadSeekCloser) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.offset += int64(n)
	return n, err
}

func (r *GCSReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	var seekTo int64
	switch whence {
	case io.SeekCurrent:
		seekTo = r.offset + offset
	case io.SeekEnd:
		seekTo = r.size + offset
	case io.SeekStart:
		seekTo = offset
	default:
		return 0, fmt.Errorf("unrecognized whence: %d", whence)
	}

	// only request a new range if we're not currently at the target position
	if seekTo != r.offset {
		err := r.r.Close()
		if err != nil {
			return 0, err
		}
		reader, err := r.object.NewRangeReader(r.ctx, seekTo, -1)
		if err != nil {
			return 0, err
		}
		r.r = reader
		r.offset = seekTo
	}
	return seekTo, nil
}

func (r *GCSReadSeekCloser) Close() error {
	return r.r.Close()
}

func NewGCSReadSeekCloser(ctx context.Context, object *storage.ObjectHandle) (*GCSReadSeekCloser, error) {
	r, err := object.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return &GCSReadSeekCloser{
		size:   r.Attrs.Size,
		object: object,
		r:      r,
		ctx:    ctx,
	}, nil
}
