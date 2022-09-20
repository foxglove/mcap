package utils

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"

	"cloud.google.com/go/storage"
)

var (
	remoteFileRegex = regexp.MustCompile(`(?P<Scheme>\w+)://(?P<Bucket>[a-z0-9_.-]+)/(?P<Filename>.*)`)
)

func GetScheme(filename string) (string, string, string) {
	match := remoteFileRegex.FindStringSubmatch(filename)
	if len(match) == 0 {
		return "", "", filename
	}
	return match[1], match[2], match[3]
}

func ReadingStdin() (bool, error) {
	stat, err := os.Stdin.Stat()
	if err != nil {
		return false, err
	}
	return stat.Mode()&os.ModeCharDevice == 0, nil
}

func StdoutRedirected() bool {
	if fi, _ := os.Stdout.Stat(); (fi.Mode() & os.ModeCharDevice) == os.ModeCharDevice {
		return false
	}
	return true
}

func GetReader(ctx context.Context, filename string) (func() error, io.ReadSeekCloser, error) {
	var rs io.ReadSeekCloser
	var err error
	close := func() error { return nil }
	scheme, bucket, path := GetScheme(filename)
	if scheme != "" {
		switch scheme {
		case "gs":
			client, err := storage.NewClient(ctx)
			if err != nil {
				return close, nil, fmt.Errorf("failed to create GCS client: %v", err)
			}
			close = client.Close
			object := client.Bucket(bucket).Object(path)
			rs, err = NewGCSReadSeekCloser(ctx, object)
			if err != nil {
				return close, nil, fmt.Errorf("failed to build read seek closer: %w", err)
			}
		default:
			return close, nil, fmt.Errorf("Unsupported remote file scheme: %s", scheme)
		}
	} else {
		rs, err = os.Open(path)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open local file")
		}
	}

	return close, rs, nil
}

func WithReader(ctx context.Context, filename string, f func(remote bool, rs io.ReadSeeker) error) error {
	var err error
	var rs io.ReadSeekCloser
	var remote bool
	scheme, bucket, path := GetScheme(filename)
	if scheme != "" {
		remote = true
		switch scheme {
		case "gs":
			client, err := storage.NewClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create GCS client: %v", err)
			}
			object := client.Bucket(bucket).Object(path)
			rs, err = NewGCSReadSeekCloser(ctx, object)
			if err != nil {
				return fmt.Errorf("failed to build read seek closer: %w", err)
			}
		default:
			return fmt.Errorf("Unsupported remote file scheme: %s", scheme)
		}
	} else {
		rs, err = os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open local file")
		}
	}
	defer rs.Close()
	return f(remote, rs)
}

func Keys[T any](m map[string]T) []string {
	keys := []string{}
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
