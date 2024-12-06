package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/jfbus/httprs"
	"github.com/olekukonko/tablewriter"
	"github.com/schollz/progressbar/v3"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob" // blank import recommended by https://gocloud.dev/howto/blob/#opening
	_ "gocloud.dev/blob/gcsblob"   // blank import recommended by https://gocloud.dev/howto/blob/#opening
	_ "gocloud.dev/blob/s3blob"    // blank import recommended by https://gocloud.dev/howto/blob/#opening
)

var (
	schemeRegex = regexp.MustCompile(`(?P<Scheme>\w+)://(?P<Path>.*)`)
	bucketRegex = regexp.MustCompile(`(?P<Bucket>[a-z0-9_.-]+)/(?P<Filename>.*)`)
)

func GetSchemeFromURI(uri string) (scheme string, path string) {
	match := schemeRegex.FindStringSubmatch(uri)
	if len(match) == 0 {
		// Probably just a raw path
		return "", uri
	}
	return match[1], match[2]
}

func GetBucketFromPath(path string) (bucket string, filename string) {
	match := bucketRegex.FindStringSubmatch(path)
	if len(match) == 0 {
		return "", path
	}
	return match[1], match[2]
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

func GetReader(ctx context.Context, uri string) (io.ReadSeekCloser, bool, error) {
	scheme, path := GetSchemeFromURI(uri)
	switch scheme {
	case "":
		// Assume that a URI without a scheme is a local path
		rs, err := os.Open(path)
		return rs, false, err
	case "http", "https":
		resp, err := http.Get(uri)
		if err != nil {
			return nil, true, err
		}
		rs := httprs.NewHttpReadSeeker(resp)
		return rs, true, nil
	default:
		// Assume that any other scheme can be handled by Go CDK
		bucket, filename := GetBucketFromPath(path)
		bucketClient, err := blob.OpenBucket(ctx, fmt.Sprintf("%v://%v", scheme, bucket))
		if err != nil {
			return nil, true, err
		}
		rs, err := NewGoCloudReadSeekCloser(ctx, bucketClient, filename)
		if err != nil {
			return nil, true, err
		}
		return rs, true, err
	}
}

func WithReader(ctx context.Context, uri string, f func(remote bool, rs io.ReadSeeker) error) error {
	reader, remote, err := GetReader(ctx, uri)
	if err != nil {
		return err
	}
	defer reader.Close()
	return f(remote, reader)
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
