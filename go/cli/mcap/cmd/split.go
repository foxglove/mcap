package cmd

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var (
	splitOutputDirectory string
	splitCompression     string
	splitChunkSize       int64
	splitIncludeCRC      bool
)

func localDiskWriteCloserProvider(directory string) func(string) (io.WriteCloser, error) {
	return func(fileName string) (io.WriteCloser, error) {
		filePath := fmt.Sprintf("%s/%s", directory, fileName)
		file, err := os.Create(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create file: %w", err)
		}
		return file, nil
	}
}

func splitMCAP(
	writeCloserProvider func(string) (io.WriteCloser, error),
	r io.Reader,
	writerOpts *mcap.WriterOptions,
) error {
	schemas := make(map[uint16]*mcap.Schema)
	outputs := make(map[uint16]*mcap.Writer)
	lexer, err := mcap.NewLexer(r, &mcap.LexerOptions{
		SkipMagic:   false,
		ValidateCRC: false,
		EmitChunks:  false,
	})
	if err != nil {
		return fmt.Errorf("failed to create lexer: %s", err)
	}

	var header *mcap.Header
	for {
		tokenType, token, err := lexer.Next(nil)
		if err != nil {
			return err
		}
		switch tokenType {
		case mcap.TokenHeader:
			header, err = mcap.ParseHeader(token)
			if err != nil {
				return fmt.Errorf("failed to parse header: %s", err)
			}
		case mcap.TokenChannel:
			channel, err := mcap.ParseChannel(token)
			if err != nil {
				return fmt.Errorf("failed to parse channel: %w", err)
			}

			// if we already know about the channel, skip. Otherwise set up a new output.
			if _, ok := outputs[channel.ID]; ok {
				continue
			}

			var normalizedTopicName string
			if len(channel.Topic) > 0 {
				normalizedTopicName = strings.ReplaceAll(channel.Topic[1:], "/", "__")
			}
			wc, err := writeCloserProvider(fmt.Sprintf("%s_%d.mcap", normalizedTopicName, channel.ID))
			if err != nil {
				return fmt.Errorf("failed to create output file: %w", err)
			}
			defer wc.Close()
			writer, err := mcap.NewWriter(wc, writerOpts)
			if err != nil {
				return fmt.Errorf("failed to create writer: %w", err)
			}
			err = writer.WriteHeader(header)
			if err != nil {
				return fmt.Errorf("failed to write header: %w", err)
			}
			outputs[channel.ID] = writer
			schema, ok := schemas[channel.SchemaID]
			if !ok {
				return fmt.Errorf("unknown schema ID: %d", channel.SchemaID)
			}
			err = writer.WriteSchema(schema)
			if err != nil {
				return fmt.Errorf("failed to write schema: %w", err)
			}
			err = writer.WriteChannel(channel)
			if err != nil {
				return fmt.Errorf("failed to write channel: %w", err)
			}
		case mcap.TokenMessage:
			message, err := mcap.ParseMessage(token)
			if err != nil {
				return fmt.Errorf("failed to parse message: %w", err)
			}
			writer, ok := outputs[message.ChannelID]
			if !ok {
				return fmt.Errorf("message with unknown channel ID: %d", message.ChannelID)
			}
			err = writer.WriteMessage(message)
			if err != nil {
				return fmt.Errorf("failed to write message: %w", err)
			}
		case mcap.TokenSchema:
			schema, err := mcap.ParseSchema(token)
			if err != nil {
				return fmt.Errorf("failed to parse schema: %w", err)
			}
			schemas[schema.ID] = schema
		case mcap.TokenDataEnd, mcap.TokenFooter:
			// close on data end, or footer to accommodate chunked or unchunked files
			for _, output := range outputs {
				err = output.Close()
				if err != nil {
					return fmt.Errorf("failed to close output: %w", err)
				}
			}
			return nil
		}
	}
}

// splitCmd represents the split command
var splitCmd = &cobra.Command{
	Use:   "split [file]",
	Short: "Split an mcap file into one file per topic.",
	Run: func(cmd *cobra.Command, args []string) {
		readingStdin, err := utils.ReadingStdin()
		if err != nil {
			die(err.Error())
		}
		var reader io.Reader
		if readingStdin {
			reader = os.Stdin
		} else {
			if len(args) < 1 {
				die("supply an input file")
			}
			f, err := os.Open(args[0])
			if err != nil {
				die(err.Error())
			}
			defer f.Close()
			reader = f
		}
		err = splitMCAP(
			localDiskWriteCloserProvider(splitOutputDirectory), reader, &mcap.WriterOptions{
				IncludeCRC:  splitIncludeCRC,
				Chunked:     true,
				ChunkSize:   8 * 1024 * 1024,
				Compression: mcap.CompressionFormat(splitCompression),
			})
		if err != nil {
			die(err.Error())
		}
	},
}

func init() {
	rootCmd.AddCommand(splitCmd)
	splitCmd.PersistentFlags().StringVarP(
		&splitOutputDirectory, "output-directory", "o", "", "Output Directory",
	)
	splitCmd.MarkPersistentFlagRequired("output-directory")
	splitCmd.PersistentFlags().StringVarP(
		&splitCompression,
		"compression",
		"",
		"zstd",
		"chunk compression algorithm (supported: zstd, lz4, none)",
	)
	splitCmd.PersistentFlags().Int64VarP(
		&splitChunkSize,
		"chunk-size",
		"",
		8*1024*1024,
		"chunk size to target in output files",
	)
	splitCmd.PersistentFlags().BoolVarP(
		&splitIncludeCRC,
		"include-crc",
		"",
		true,
		"include chunk CRC checksums in output files",
	)
}
