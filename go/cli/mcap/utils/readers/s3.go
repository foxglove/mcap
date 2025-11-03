package readers

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Automatically register S3 reader when imported.
func init() {
	RegisterReader("s3", newS3Reader)
}

// Factory for S3 readers (called by registry).
func newS3Reader(ctx context.Context, bucket, path string) (func() error, io.ReadSeekCloser, error) {
	// Try anonymous first
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
	)
	if err != nil {
		return func() error { return nil }, nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)
	rs, err := NewS3ReadSeekCloser(ctx, client, bucket, path)
	if err == nil {
		return rs.Close, rs, nil
	}

	// Fallback to authenticated config
	cfg, err = config.LoadDefaultConfig(ctx)
	if err != nil {
		return func() error { return nil }, nil, fmt.Errorf("failed to load authenticated AWS config: %w", err)
	}

	client = s3.NewFromConfig(cfg)
	rs, err = NewS3ReadSeekCloser(ctx, client, bucket, path)
	if err != nil {
		return func() error { return nil }, nil, fmt.Errorf("failed to create S3 reader: %w", err)
	}

	return rs.Close, rs, nil
}

// S3ReadSeekCloser implements io.ReadSeekCloser for S3 objects.
type S3ReadSeekCloser struct {
	ctx    context.Context
	client *s3.Client
	bucket string
	key    string
	reader io.ReadCloser
	size   int64
	offset int64
}

// NewS3ReadSeekCloser creates a seekable reader for an S3 object.
func NewS3ReadSeekCloser(ctx context.Context, client *s3.Client, bucket, key string) (*S3ReadSeekCloser, error) {
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to head S3 object: %w", err)
	}

	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open S3 object: %w", err)
	}

	return &S3ReadSeekCloser{
		ctx:    ctx,
		client: client,
		bucket: bucket,
		key:    key,
		reader: resp.Body,
		size:   *head.ContentLength,
		offset: 0,
	}, nil
}

func (r *S3ReadSeekCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.offset += int64(n)
	return n, err
}

func (r *S3ReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
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

	rangeHeader := fmt.Sprintf("bytes=%d-", target)
	resp, err := r.client.GetObject(r.ctx, &s3.GetObjectInput{
		Bucket: &r.bucket,
		Key:    &r.key,
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to reopen S3 object: %w", err)
	}

	r.reader = resp.Body
	r.offset = target
	return target, nil
}

func (r *S3ReadSeekCloser) Close() error {
	return r.reader.Close()
}
