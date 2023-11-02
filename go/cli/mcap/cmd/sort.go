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
	sortOutputFile string
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

func sortFile(w io.Writer, r io.ReadSeeker) error {
	reader, err := mcap.NewReader(r)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}
	writer, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Chunked:     true,
		Compression: mcap.CompressionZSTD,
		ChunkSize:   4 * 1024 * 1024,
	})
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}
	info, err := reader.Info()
	if err != nil {
		return errUnindexedFile{err}
	}

	err = writer.WriteHeader(info.Header)
	if err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// handle the attachments and metadata metadata first; physical location in
	// the file is irrelevant but order is preserved.
	for _, index := range info.AttachmentIndexes {
		ar, err := reader.GetAttachment(index.Offset)
		if err != nil {
			return fmt.Errorf("failed to read attachment: %w", err)
		}
		err = writer.WriteAttachment(&mcap.Attachment{
			Name:       index.Name,
			MediaType:  index.MediaType,
			CreateTime: index.CreateTime,
			LogTime:    index.LogTime,
			DataSize:   index.DataSize,
			Data:       ar.Data(),
		})
		if err != nil {
			return fmt.Errorf("failed to read attachment: %w", err)
		}
	}
	for _, index := range info.MetadataIndexes {
		metadata, err := reader.GetMetadata(index.Offset)
		if err != nil {
			return fmt.Errorf("failed to read attachment: %w", err)
		}
		err = writer.WriteMetadata(metadata)
		if err != nil {
			return fmt.Errorf("failed to read attachment: %w", err)
		}
	}

	it, err := reader.Messages(mcap.UsingIndex(true), mcap.InOrder(mcap.LogTimeOrder))
	if err != nil {
		return fmt.Errorf("failed to read messages: %w", err)
	}
	schemas := make(map[uint16]*mcap.Schema)
	channels := make(map[uint16]*mcap.Schema)
	for {
		schema, channel, message, err := it.Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
		}
		if _, ok := schemas[schema.ID]; !ok {
			err := writer.WriteSchema(schema)
			if err != nil {
				return fmt.Errorf("failed to write schema: %w", err)
			}
		}
		if _, ok := channels[channel.ID]; !ok {
			err := writer.WriteChannel(channel)
			if err != nil {
				return fmt.Errorf("failed to write channel: %w", err)
			}
		}
		err = writer.WriteMessage(message)
		if err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}
	}

	return writer.Close()
}

var sortCmd = &cobra.Command{
	Use:   "sort [file] -o output.mcap",
	Short: "Read an MCAP file and write the messages out physically sorted on time",
	Run: func(cmd *cobra.Command, args []string) {
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
				die("Error reading file index: %s. You may need to run `mcap recover` if the file is corrupt.", err)
			}
			die("failed to sort file: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(sortCmd)
	sortCmd.PersistentFlags().StringVarP(
		&sortOutputFile,
		"output-file",
		"o",
		"",
		"output file",
	)
	err := sortCmd.MarkPersistentFlagRequired("output-file")
	if err != nil {
		die("failed to mark flag required: %s", err)
	}
}
