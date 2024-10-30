package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"time"

	"cloud.google.com/go/storage"
	"github.com/olekukonko/tablewriter"
	"github.com/schollz/progressbar/v3"
)

var (
	remoteFileRegex = regexp.MustCompile(`(?P<Scheme>\w+)://(?P<Bucket>[a-z0-9_.-]+)/(?P<Filename>.*)`)
)

func GetScheme(filename string) (match1 string, match2 string, match3 string) {
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
	closeReader := func() error { return nil }
	scheme, bucket, path := GetScheme(filename)
	if scheme != "" {
		switch scheme {
		case "gs":
			client, err := storage.NewClient(ctx)
			if err != nil {
				return closeReader, nil, fmt.Errorf("failed to create GCS client: %w", err)
			}
			closeReader = client.Close
			object := client.Bucket(bucket).Object(path)
			rs, err = NewGCSReadSeekCloser(ctx, object)
			if err != nil {
				return closeReader, nil, fmt.Errorf("failed to build read seek closer: %w", err)
			}
		default:
			return closeReader, nil, fmt.Errorf("unsupported remote file scheme: %s", scheme)
		}
	} else {
		rs, err = os.Open(path)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open local file")
		}
	}

	return closeReader, rs, nil
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
				return fmt.Errorf("failed to create GCS client: %w", err)
			}
			object := client.Bucket(bucket).Object(path)
			rs, err = NewGCSReadSeekCloser(ctx, object)
			if err != nil {
				return fmt.Errorf("failed to build read seek closer: %w", err)
			}
		default:
			return fmt.Errorf("unsupported remote file scheme: %s", scheme)
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

func FormatTable(w io.Writer, rows [][]string) {
	tw := tablewriter.NewWriter(w)
	tw.SetBorder(false)
	tw.SetAutoWrapText(false)
	tw.SetAlignment(tablewriter.ALIGN_LEFT)
	tw.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	tw.SetColumnSeparator("")
	tw.SetTablePadding("\t")
	tw.SetNoWhiteSpace(true)

	tw.AppendBulk(rows)
	tw.Render()
}

func Keys[T any](m map[string]T) []string {
	keys := []string{}
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func PrettyJSON(data []byte) (string, error) {
	indented := &bytes.Buffer{}
	err := json.Indent(indented, data, "", "  ")
	if err != nil {
		return "", err
	}
	return indented.String(), nil
}

// DefaultString returns the first of the provided strings that is nonempty, or
// an empty string if they are all empty.
func DefaultString(strings ...string) string {
	for _, s := range strings {
		if s != "" {
			return s
		}
	}
	return ""
}

// NewProgressBar returns an instance of progressbar.ProgresBar.
// `max` is the denominator of the progress.
func NewProgressBar(max int64) *progressbar.ProgressBar {
	return progressbar.NewOptions64(
		max,
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionSetWidth(10),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetRenderBlankState(true),
	)
}
