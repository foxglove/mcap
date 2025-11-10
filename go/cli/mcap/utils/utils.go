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

	"github.com/foxglove/mcap/go/cli/mcap/utils/readers"

	"github.com/olekukonko/tablewriter"
	"github.com/schollz/progressbar/v3"
)

// remoteFileRegex parses URIs like "gs://bucket/path/to/file"
var (
	remoteFileRegex = regexp.MustCompile(`(?P<Scheme>\w+)://(?P<Bucket>[a-z0-9_.-]+)/(?P<Filename>.*)`)
)

// GetScheme splits a URI into (scheme, bucket, path).
func GetScheme(filename string) (scheme, bucket, path string) {
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

// Eprintf formats according to a format specifier and writes to standard error.
// It returns the number of bytes written and any write error encountered.
func EprintF(format string, a ...any) (n int, err error) {
	return fmt.Fprintf(os.Stderr, format, a...)
}

// Eprintln formats using the default formats for its operands and writes to standard error.
// Spaces are always added between operands and a newline is appended.
// It returns the number of bytes written and any write error encountered.
func Eprintln(a ...any) (n int, err error) {
	return fmt.Fprintln(os.Stderr, a...)
}

// GetReader returns a ReadSeekCloser for local or remote sources.
// It delegates remote handling to the readers registry.
func GetReader(ctx context.Context, filename string) (func() error, io.ReadSeekCloser, error) {
	scheme, bucket, path := GetScheme(filename)
	return readers.GetReader(ctx, scheme, bucket, path)
}

// WithReader runs a function with a ReadSeeker for the given source.
// It automatically closes after use.
func WithReader(ctx context.Context, filename string, f func(remote bool, rs io.ReadSeeker) error) error {
	scheme, bucket, path := GetScheme(filename)
	return readers.WithReader(ctx, scheme, bucket, path, f)
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
