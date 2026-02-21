package readers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/volcengine/ve-tos-golang-sdk/v2/tos"
)

var (
	tosEndpointEnvVars   = []string{"MCAP_TOS_ENDPOINT", "TOS_ENDPOINT"}
	tosRegionEnvVars     = []string{"MCAP_TOS_REGION", "TOS_REGION"}
	tosAccessKeyAliasEnv = []string{"MCAP_TOS_ACCESS_KEY", "TOS_ACCESS_KEY"}
	tosSecretKeyAliasEnv = []string{"MCAP_TOS_SECRET_KEY", "TOS_SECRET_KEY"}
)

// Automatically register TOS reader when imported.
func init() {
	RegisterReader("tos", newTOSReader)
}

// Factory for TOS readers (called by registry).
func newTOSReader(ctx context.Context, bucket, path string) (func() error, io.ReadSeekCloser, error) {
	endpoint := firstNonEmptyEnv(tosEndpointEnvVars...)
	region := firstNonEmptyEnv(tosRegionEnvVars...)
	if endpoint == "" && region == "" {
		return func() error { return nil }, nil, fmt.Errorf(
			"TOS endpoint or region must be configured (set MCAP_TOS_ENDPOINT/TOS_ENDPOINT or MCAP_TOS_REGION/TOS_REGION)",
		)
	}

	opts := []tos.ClientOption{tos.WithRegion(region)}

	useEnvCreds, err := configureTOSEnvCredentials()
	if err != nil {
		return func() error { return nil }, nil, fmt.Errorf("failed to configure TOS credentials: %w", err)
	}

	if useEnvCreds {
		opts = append(opts, tos.WithCredentialsProvider(&tos.EnvCredentialsProvider{}))
	}

	client, err := tos.NewClientV2(endpoint, opts...)
	if err != nil {
		return func() error { return nil }, nil, fmt.Errorf("failed to create TOS client: %w", err)
	}
	rs, err := NewTOSReadSeekCloser(ctx, client, bucket, path)
	if err != nil {
		client.Close()
		return func() error { return nil }, nil, fmt.Errorf("failed to create TOS reader: %w", err)
	}

	return func() error {
		client.Close()
		return nil
	}, rs, nil
}

// TOSReadSeekCloser implements io.ReadSeekCloser for objects on TOS.
type TOSReadSeekCloser struct {
	ctx    context.Context
	client *tos.ClientV2
	bucket string
	key    string
	reader io.ReadCloser
	size   int64
	offset int64
}

// NewTOSReadSeekCloser creates a seekable reader backed by TOS.
func NewTOSReadSeekCloser(ctx context.Context, client *tos.ClientV2, bucket, key string) (*TOSReadSeekCloser, error) {
	output, err := client.GetObjectV2(ctx, &tos.GetObjectV2Input{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open TOS object: %w", err)
	}

	return &TOSReadSeekCloser{
		ctx:    ctx,
		client: client,
		bucket: bucket,
		key:    key,
		reader: output.Content,
		size:   output.ContentLength,
		offset: 0,
	}, nil
}

// Read implements io.Reader.
func (r *TOSReadSeekCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.offset += int64(n)
	return n, err
}

// Seek implements io.Seeker backed by range requests.
func (r *TOSReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
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

	if target < 0 || target > r.size {
		return 0, fmt.Errorf("seek out of bounds: %d", target)
	}
	if target == r.offset {
		return target, nil
	}
	if target == r.size {
		_ = r.reader.Close()
		r.reader = io.NopCloser(bytes.NewReader(nil))
		r.offset = target
		return target, nil
	}

	_ = r.reader.Close()
	input := &tos.GetObjectV2Input{
		Bucket: r.bucket,
		Key:    r.key,
	}
	if target > 0 {
		input.RangeStart = target
		input.RangeEnd = r.size - 1
	}
	output, err := r.client.GetObjectV2(r.ctx, input)
	if err != nil {
		return 0, fmt.Errorf("failed to reopen TOS object: %w", err)
	}
	r.reader = output.Content
	r.offset = target
	return target, nil
}

// Close closes the current reader.
func (r *TOSReadSeekCloser) Close() error {
	return r.reader.Close()
}

func firstNonEmptyEnv(keys ...string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return ""
}

func configureTOSEnvCredentials() (bool, error) {
	if err := ensureEnvVarFromAliases("TOS_ACCESS_KEY", tosAccessKeyAliasEnv); err != nil {
		return false, err
	}
	if err := ensureEnvVarFromAliases("TOS_SECRET_KEY", tosSecretKeyAliasEnv); err != nil {
		return false, err
	}
	return os.Getenv("TOS_ACCESS_KEY") != "" && os.Getenv("TOS_SECRET_KEY") != "", nil
}

func ensureEnvVarFromAliases(target string, aliases []string) error {
	if os.Getenv(target) != "" {
		return nil
	}
	for _, alias := range aliases {
		if value := os.Getenv(alias); value != "" {
			return os.Setenv(target, value)
		}
	}
	return nil
}
