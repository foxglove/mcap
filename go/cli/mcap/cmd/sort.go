package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var (
	sortOutputFile  string
	sortChunkSize   int64
	sortCompression string
	sortIncludeCRC  bool
	sortChunked     bool
)

type errUnindexedFile struct {
	err error
}

func (e errUnindexedFile) Error() string {
	return e.err.Error()
}

func (e errUnindexedFile) Is(tgt error) bool {
	_, ok := tgt.(errUnindexedFile)
	return ok
}

func fileHasNoMessages(r io.ReadSeeker) (bool, error) {
	_, err := r.Seek(0, io.SeekStart)
	if err != nil {
		return false, err
	}
	reader, err := mcap.NewReader(r)
	if err != nil {
		return false, err
	}
	defer reader.Close()
	it, err := reader.Messages(mcap.UsingIndex(false), mcap.InOrder(mcap.FileOrder))
	if err != nil {
		return false, err
	}
	_, _, _, err = it.Next2(nil)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func sortFile(w io.Writer, r io.ReadSeeker) error {
	reader, err := mcap.NewReader(r)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}
	writer, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Chunked:     sortChunked,
		Compression: mcap.CompressionFormat(sortCompression),
		ChunkSize:   sortChunkSize,
		IncludeCRC:  sortIncludeCRC,
	})
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}
	info, err := reader.Info()
	if err != nil {
		return errUnindexedFile{err}
	}

	isEmpty, err := fileHasNoMessages(r)
	if err != nil {
		return fmt.Errorf("failed to check if file is empty: %w", err)
	}

	if len(info.ChunkIndexes) == 0 && !isEmpty {
		return errUnindexedFile{errors.New("no chunk index records")}
	}

	err = writer.WriteHeader(info.Header)
	if err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// handle the attachments and metadata metadata first; physical location in
	// the file is irrelevant but order is preserved.
	for _, index := range info.AttachmentIndexes {
		attReader, err := reader.GetAttachmentReader(index.Offset)
		if err != nil {
			return fmt.Errorf("failed to read attachment: %w", err)
		}
		err = writer.WriteAttachment(&mcap.Attachment{
			Name:       index.Name,
			MediaType:  index.MediaType,
			CreateTime: index.CreateTime,
			LogTime:    index.LogTime,
			DataSize:   index.DataSize,
			Data:       attReader.Data(),
		})
		if err != nil {
			return fmt.Errorf("failed to write attachment: %w", err)
		}
	}
	for _, index := range info.MetadataIndexes {
		metadata, err := reader.GetMetadata(index.Offset)
		if err != nil {
			return fmt.Errorf("failed to read metadata: %w", err)
		}
		err = writer.WriteMetadata(metadata)
		if err != nil {
			return fmt.Errorf("failed to write metadata: %w", err)
		}
	}

	it, err := reader.Messages(mcap.UsingIndex(true), mcap.InOrder(mcap.LogTimeOrder))
	if err != nil {
		return fmt.Errorf("failed to read messages: %w", err)
	}
	schemas := make(map[uint16]*mcap.Schema)
	channels := make(map[uint16]*mcap.Schema)
	message := mcap.Message{}
	for {
		schema, channel, _, err := it.Next2(&message)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
		}
		if schema != nil {
			if _, ok := schemas[schema.ID]; !ok {
				err := writer.WriteSchema(schema)
				if err != nil {
					return fmt.Errorf("failed to write schema: %w", err)
				}
			}
		}
		if _, ok := channels[channel.ID]; !ok {
			err := writer.WriteChannel(channel)
			if err != nil {
				return fmt.Errorf("failed to write channel: %w", err)
			}
		}
		err = writer.WriteMessage(&message)
		if err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}
	}

	return writer.Close()
}

var sortCmd = &cobra.Command{
	Use:   "sort [file] -o output.mcap",
	Short: "Read an MCAP file and write the messages out physically sorted on log time",
	Run: func(_ *cobra.Command, args []string) {
		if len(args) != 1 {
			die("supply a file")
		}
		f, err := os.Open(args[0])
		if err != nil {
			die("failed to open file: %s", err)
		}
		defer f.Close()

		output, err := os.Create(sortOutputFile)
		if err != nil {
			die("failed to open output: %s", err)
		}
		err = sortFile(output, f)
		if err != nil {
			if errors.Is(err, errUnindexedFile{}) {
				die("Error reading file index: %s. "+
					"You may need to run `mcap recover` if the file is corrupt or not chunk indexed.", err)
			}
			die("failed to sort file: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(sortCmd)
	sortCmd.PersistentFlags().StringVarP(&sortOutputFile, "output-file", "o", "", "output file")
	sortCmd.PersistentFlags().Int64VarP(&sortChunkSize, "chunk-size", "", 4*1024*1024, "chunk size")
	sortCmd.PersistentFlags().StringVarP(&sortCompression, "compression", "", "zstd", "chunk compression algorithm")
	sortCmd.PersistentFlags().BoolVarP(&sortIncludeCRC, "include-crc", "", true, "include chunk CRCs in output")
	sortCmd.PersistentFlags().BoolVarP(&sortChunked, "chunked", "", true, "create an indexed and chunk-compressed output")
	err := sortCmd.MarkPersistentFlagRequired("output-file")
	if err != nil {
		die("failed to mark flag required: %s", err)
	}
}
