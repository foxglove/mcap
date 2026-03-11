package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var (
	renameChannelFrom        string
	renameChannelTo          string
	renameChannelOutput      string
	renameChannelCompression string
	renameChannelChunkSize   int64
	renameChannelIncludeCRC  bool
)

type renameOpts struct {
	compression mcap.CompressionFormat
	chunkSize   int64
	includeCRC  bool
}

func rewriteChannelTopics(
	dst io.Writer,
	src io.Reader,
	fromTopic string,
	toTopic string,
	opts renameOpts,
) (int, error) {
	if fromTopic == toTopic {
		return 0, fmt.Errorf("source and target topics are identical: %q", fromTopic)
	}
	writer, err := mcap.NewWriter(dst, &mcap.WriterOptions{
		Chunked:     true,
		ChunkSize:   opts.chunkSize,
		Compression: opts.compression,
		IncludeCRC:  opts.includeCRC,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to construct output writer: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = writer.Close()
		}
	}()

	lexer, err := mcap.NewLexer(src, &mcap.LexerOptions{
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
		return 0, fmt.Errorf("failed to construct input lexer: %w", err)
	}

	// Buffer schemas and channels so we can check for topic collisions across
	// all channels before writing any of them. This prevents a rename from
	// silently producing duplicate topics when the source channel appears
	// before the target channel in the file.
	type pendingRecord struct {
		tokenType mcap.TokenType
		token     []byte
	}
	var pending []pendingRecord
	var channels []*mcap.Channel
	flushed := false

	// flushPending checks for collisions, applies renames, and writes all
	// buffered schemas and channels to the writer.
	flushPending := func() (int, error) {
		if flushed || len(pending) == 0 {
			return 0, nil
		}
		flushed = true
		// Check that renaming won't create a duplicate topic.
		for _, ch := range channels {
			if ch.Topic == toTopic {
				return 0, fmt.Errorf("target topic %q already exists in the file", toTopic)
			}
		}
		count := 0
		for _, rec := range pending {
			switch rec.tokenType {
			case mcap.TokenSchema:
				schema, err := mcap.ParseSchema(rec.token)
				if err != nil {
					return count, fmt.Errorf("failed to parse schema: %w", err)
				}
				if err := writer.WriteSchema(schema); err != nil {
					return count, fmt.Errorf("failed to write schema: %w", err)
				}
			case mcap.TokenChannel:
				channel, err := mcap.ParseChannel(rec.token)
				if err != nil {
					return count, fmt.Errorf("failed to parse channel: %w", err)
				}
				if channel.Topic == fromTopic {
					channel.Topic = toTopic
					count++
				}
				if err := writer.WriteChannel(channel); err != nil {
					return count, fmt.Errorf("failed to write channel: %w", err)
				}
			}
		}
		pending = nil
		channels = nil
		return count, nil
	}

	// renameAndWriteChannel applies the topic rename to a single channel
	// and writes it. Used for late channels that arrive after the initial flush.
	renameAndWriteChannel := func(channel *mcap.Channel) (int, error) {
		if channel.Topic == toTopic {
			return 0, fmt.Errorf("target topic %q already exists in the file", toTopic)
		}
		count := 0
		if channel.Topic == fromTopic {
			channel.Topic = toTopic
			count++
		}
		if err := writer.WriteChannel(channel); err != nil {
			return count, fmt.Errorf("failed to write channel: %w", err)
		}
		return count, nil
	}

	renamed := 0
	for {
		tokenType, token, err := lexer.Next(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return renamed, fmt.Errorf("failed to read input MCAP: %w", err)
		}
		switch tokenType {
		case mcap.TokenHeader:
			header, err := mcap.ParseHeader(token)
			if err != nil {
				return renamed, fmt.Errorf("failed to parse header: %w", err)
			}
			if err := writer.WriteHeader(header); err != nil {
				return renamed, fmt.Errorf("failed to write header: %w", err)
			}
		case mcap.TokenSchema:
			if flushed {
				// Late schema (after the first message) — write directly.
				schema, err := mcap.ParseSchema(token)
				if err != nil {
					return renamed, fmt.Errorf("failed to parse schema: %w", err)
				}
				if err := writer.WriteSchema(schema); err != nil {
					return renamed, fmt.Errorf("failed to write schema: %w", err)
				}
			} else {
				tokenCopy := make([]byte, len(token))
				copy(tokenCopy, token)
				pending = append(pending, pendingRecord{tokenType, tokenCopy})
			}
		case mcap.TokenChannel:
			channel, err := mcap.ParseChannel(token)
			if err != nil {
				return renamed, fmt.Errorf("failed to parse channel: %w", err)
			}
			if flushed {
				// Late channel (after the first message) — rename and write directly.
				n, err := renameAndWriteChannel(channel)
				renamed += n
				if err != nil {
					return renamed, err
				}
			} else {
				channels = append(channels, channel)
				tokenCopy := make([]byte, len(token))
				copy(tokenCopy, token)
				pending = append(pending, pendingRecord{tokenType, tokenCopy})
			}
		case mcap.TokenMessage:
			// First message means the initial batch of channels has been seen.
			n, err := flushPending()
			renamed += n
			if err != nil {
				return renamed, err
			}
			message, err := mcap.ParseMessage(token)
			if err != nil {
				return renamed, fmt.Errorf("failed to parse message: %w", err)
			}
			if err := writer.WriteMessage(message); err != nil {
				return renamed, fmt.Errorf("failed to write message: %w", err)
			}
		case mcap.TokenMetadata:
			metadata, err := mcap.ParseMetadata(token)
			if err != nil {
				return renamed, fmt.Errorf("failed to parse metadata: %w", err)
			}
			if err := writer.WriteMetadata(metadata); err != nil {
				return renamed, fmt.Errorf("failed to write metadata: %w", err)
			}
		case mcap.TokenDataEnd:
			// Flush any buffered schemas/channels if the file had no messages.
			n, err := flushPending()
			renamed += n
			if err != nil {
				return renamed, err
			}
			if err := writer.Close(); err != nil {
				return renamed, fmt.Errorf("failed to finalize output: %w", err)
			}
			closed = true
			return renamed, nil
		}
	}
	if err := writer.Close(); err != nil {
		return renamed, fmt.Errorf("failed to finalize output: %w", err)
	}
	closed = true
	return renamed, nil
}

func renameChannelInFile(
	inputPath string,
	outputPath string,
	fromTopic string,
	toTopic string,
	opts renameOpts,
) error {
	resolvedInput, err := filepath.Abs(inputPath)
	if err != nil {
		return fmt.Errorf("failed to resolve input path: %w", err)
	}

	input, err := os.Open(resolvedInput)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}

	var resolvedTarget string
	if outputPath == "" {
		resolvedTarget = resolvedInput
	} else {
		resolvedTarget, err = filepath.Abs(outputPath)
		if err != nil {
			input.Close()
			return fmt.Errorf("failed to resolve output path: %w", err)
		}
	}

	inPlace := resolvedTarget == resolvedInput
	if inPlace {
		// Preserve the original file's permissions after replacing it.
		fi, err := input.Stat()
		if err != nil {
			input.Close()
			return fmt.Errorf("failed to stat input file: %w", err)
		}
		originalMode := fi.Mode()

		tmpfile, err := os.CreateTemp(filepath.Dir(resolvedInput), "mcap-rename-*")
		if err != nil {
			input.Close()
			return fmt.Errorf("failed to create temporary file: %w", err)
		}
		tmpname := tmpfile.Name()
		renamed, rewriteErr := rewriteChannelTopics(tmpfile, input, fromTopic, toTopic, opts)
		// Close both files before any rename or cleanup.
		input.Close()
		closeErr := tmpfile.Close()
		if rewriteErr != nil {
			_ = os.Remove(tmpname)
			return rewriteErr
		}
		if closeErr != nil {
			_ = os.Remove(tmpname)
			return fmt.Errorf("failed to close temporary file: %w", closeErr)
		}
		if renamed == 0 {
			_ = os.Remove(tmpname)
			return fmt.Errorf("topic %q was not found", fromTopic)
		}
		if err := os.Chmod(tmpname, originalMode); err != nil {
			_ = os.Remove(tmpname)
			return fmt.Errorf("failed to preserve file permissions: %w", err)
		}
		if err := os.Rename(tmpname, resolvedInput); err != nil {
			_ = os.Remove(tmpname)
			return fmt.Errorf("failed to replace input file: %w", err)
		}
		return nil
	}

	// Non-in-place path: input can be deferred.
	defer input.Close()

	output, err := os.Create(resolvedTarget)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	renamed, rewriteErr := rewriteChannelTopics(output, input, fromTopic, toTopic, opts)
	closeErr := output.Close()
	if rewriteErr != nil {
		_ = os.Remove(resolvedTarget)
		return rewriteErr
	}
	if closeErr != nil {
		_ = os.Remove(resolvedTarget)
		return fmt.Errorf("failed to close output file: %w", closeErr)
	}
	if renamed == 0 {
		_ = os.Remove(resolvedTarget)
		return fmt.Errorf("topic %q was not found", fromTopic)
	}
	return nil
}

var renameChannelCmd = &cobra.Command{
	Use:   "channel [file]",
	Short: "Rename a channel topic in an MCAP file",
	Long: `Rename a channel's topic in an MCAP file. By default, the file is modified
in place. Use --output to write to a new file instead.

Examples:
  mcap rename channel input.mcap --from /tf --to /tf_renamed
  mcap rename channel input.mcap --from /tf --to /tf_renamed --output output.mcap`,
	Args: cobra.ExactArgs(1),
	Run: func(_ *cobra.Command, args []string) {
		compression := renameChannelCompression
		if compression == compressionNoneAlias {
			compression = ""
		}
		opts := renameOpts{
			compression: mcap.CompressionFormat(compression),
			chunkSize:   renameChannelChunkSize,
			includeCRC:  renameChannelIncludeCRC,
		}
		err := renameChannelInFile(args[0], renameChannelOutput, renameChannelFrom, renameChannelTo, opts)
		if err != nil {
			die("failed to rename channel: %s", err)
		}
	},
}

func init() {
	renameCmd.AddCommand(renameChannelCmd)
	renameChannelCmd.PersistentFlags().StringVar(
		&renameChannelFrom, "from", "", "existing topic name to rename",
	)
	renameChannelCmd.PersistentFlags().StringVar(
		&renameChannelTo, "to", "", "new topic name",
	)
	renameChannelCmd.PersistentFlags().StringVarP(
		&renameChannelOutput, "output", "o", "",
		"write renamed MCAP to a new file (default: in-place)",
	)
	renameChannelCmd.PersistentFlags().StringVar(
		&renameChannelCompression, "compression", "zstd",
		"chunk compression algorithm (supported: zstd, lz4, none)",
	)
	renameChannelCmd.PersistentFlags().Int64Var(
		&renameChannelChunkSize, "chunk-size", 8*1024*1024,
		"chunk size to target",
	)
	renameChannelCmd.PersistentFlags().BoolVar(
		&renameChannelIncludeCRC, "include-crc", true,
		"include chunk CRC checksums in output",
	)
	err := renameChannelCmd.MarkPersistentFlagRequired("from")
	if err != nil {
		die("failed to mark flag required: %s", err)
	}
	err = renameChannelCmd.MarkPersistentFlagRequired("to")
	if err != nil {
		die("failed to mark flag required: %s", err)
	}
}
