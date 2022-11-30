package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"

	"cloud.google.com/go/storage"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/olekukonko/tablewriter"
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

func FormatTable(w io.Writer, rows [][]string) {
	tw := tablewriter.NewWriter(w)
	tw.SetBorder(false)
	tw.SetAutoWrapText(false)
	tw.SetAlignment(tablewriter.ALIGN_LEFT)
	tw.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	tw.SetColumnSeparator("")
	tw.AppendBulk(rows)
	tw.Render()
}

func inferWriterOptions(info *mcap.Info) *mcap.WriterOptions {
	// assume if there are no chunk indexes, the file is not chunked. This
	// assumption may be invalid if the file is chunked but not indexed.
	if len(info.ChunkIndexes) == 0 {
		return &mcap.WriterOptions{
			Chunked: false,
		}
	}
	// if there are chunk indexes, create a chunked output with attributes
	// approximating those of the first chunk.
	idx := info.ChunkIndexes[0]
	return &mcap.WriterOptions{
		IncludeCRC:  true,
		Chunked:     true,
		ChunkSize:   int64(idx.ChunkLength),
		Compression: idx.Compression,
	}
}

// RewriteMCAP rewrites the mcap file wrapped by the provided ReadSeeker and
// performs the operations described by the supplied fns at the end of writing.
// It is used for adding metadata and attachments to an existing file. In the
// future this can be optimized to rewrite only the summary section, which
// should make it run much faster but will require some tricky modifications of
// indexes pointing into the summary section.
func RewriteMCAP(w io.Writer, r io.ReadSeeker, fns ...func(writer *mcap.Writer) error) error {
	reader, err := mcap.NewReader(r)
	if err != nil {
		return fmt.Errorf("failed to open mcap reader: %w", err)
	}
	defer reader.Close()
	info, err := reader.Info()
	if err != nil {
		return fmt.Errorf("failed to get mcap info")
	}
	opts := inferWriterOptions(info)
	writer, err := mcap.NewWriter(w, opts)
	if err != nil {
		return fmt.Errorf("failed to construct mcap writer: %w", err)
	}
	defer writer.Close()
	if err := writer.WriteHeader(info.Header); err != nil {
		return fmt.Errorf("failed to rewrite header: %w", err)
	}
	_, err = r.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to reader start: %w", err)
	}
	lexer, err := mcap.NewLexer(r, &mcap.LexerOptions{
		SkipMagic:         false,
		ValidateChunkCRCs: false,
		EmitChunks:        false,
		AttachmentCallback: func(ar *mcap.AttachmentReader) error {
			return writer.WriteAttachment(&mcap.Attachment{
				LogTime:    ar.LogTime,
				CreateTime: ar.CreateTime,
				Name:       ar.Name,
				MediaType:  ar.MediaType,
				DataSize:   ar.DataSize,
				Data:       ar.Data(),
			})
		},
	})
	if err != nil {
		return fmt.Errorf("failed to construct lexer: %w", err)
	}
	defer lexer.Close()
	buf := make([]byte, 1024)
	schemas := make(map[uint16]bool)
	channels := make(map[uint16]bool)
	for {
		tokenType, token, err := lexer.Next(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to pull next record: %w", err)
		}
		if len(token) > len(buf) {
			buf = token
		}
		switch tokenType {
		case mcap.TokenChannel:
			record, err := mcap.ParseChannel(token)
			if err != nil {
				return fmt.Errorf("failed to parse channel: %w", err)
			}
			if !channels[record.ID] {
				err := writer.WriteChannel(record)
				if err != nil {
					return fmt.Errorf("failed to write channel: %w", err)
				}
				channels[record.ID] = true
			}
		case mcap.TokenSchema:
			record, err := mcap.ParseSchema(token)
			if err != nil {
				return fmt.Errorf("failed to parse schema: %w", err)
			}
			if !schemas[record.ID] {
				err := writer.WriteSchema(record)
				if err != nil {
					return fmt.Errorf("failed to write schema: %w", err)
				}
				schemas[record.ID] = true
			}
		case mcap.TokenMessage:
			record, err := mcap.ParseMessage(token)
			if err != nil {
				return fmt.Errorf("failed to parse message: %w", err)
			}
			err = writer.WriteMessage(record)
			if err != nil {
				return fmt.Errorf("failed to write message: %w", err)
			}
		case mcap.TokenMetadata:
			record, err := mcap.ParseMetadata(token)
			if err != nil {
				return fmt.Errorf("failed to parse metadata: %w", err)
			}
			err = writer.WriteMetadata(record)
			if err != nil {
				return fmt.Errorf("failed to write metadata: %w", err)
			}
		default:
			continue
		}
	}
	for _, f := range fns {
		err = f(writer)
		if err != nil {
			return fmt.Errorf("failed to apply writer function: %w", err)
		}
	}
	return nil
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
