package utils

import (
	"context"
	"fmt"
	"io"

	"gocloud.dev/blob"
)

type GoCloudReadSeekCloser struct {
	size   int64
	key    string
	ctx    context.Context
	offset int64
	r      io.ReadCloser
	bucket *blob.Bucket
}

func (r *GoCloudReadSeekCloser) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.offset += int64(n)
	return n, err
}

func (r *GoCloudReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
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
		reader, err := r.bucket.NewRangeReader(r.ctx, r.key, seekTo, -1, nil)
		if err != nil {
			return 0, err
		}
		r.r = reader
		r.offset = seekTo
	}
	return seekTo, nil
}

func (r *GoCloudReadSeekCloser) Close() error {
	return r.r.Close()
}

func NewGoCloudReadSeekCloser(ctx context.Context, bucket *blob.Bucket, key string) (*GoCloudReadSeekCloser, error) {
	r, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		return nil, err
	}

	return &GoCloudReadSeekCloser{
		size:   r.Size(),
		key:    key,
		r:      r,
		ctx:    ctx,
		bucket: bucket,
	}, nil
}
