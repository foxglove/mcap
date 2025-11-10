package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

type recoverOptions struct {
	decodeChunk bool
	chunkSize   int64
	compression mcap.CompressionFormat
}

func recoverRun(
	r io.Reader,
	w io.Writer,
	ops *recoverOptions,
) error {
	decodeChunk := ops.decodeChunk
	mcapWriter, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Chunked:     true,
		ChunkSize:   ops.chunkSize,
		Compression: ops.compression,
	})
	if err != nil {
		return err
	}

	info := &mcap.Info{
		Statistics: &mcap.Statistics{
			ChannelMessageCounts: make(map[uint16]uint64),
		},
		Channels: make(map[uint16]*mcap.Channel),
		Schemas:  make(map[uint16]*mcap.Schema),
	}

	defer func() {
		mcapWriter.Statistics.MessageCount += info.Statistics.MessageCount
		for channelID, count := range info.Statistics.ChannelMessageCounts {
			mcapWriter.Statistics.ChannelMessageCounts[channelID] += count
		}

		for _, schema := range info.Schemas {
			mcapWriter.AddSchema(schema)
		}
		for _, channel := range info.Channels {
			mcapWriter.AddChannel(channel)
		}

		err := mcapWriter.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to close mcap writer: %v\n", err)
			return
		}
		fmt.Fprintf(
			os.Stderr,
			"Recovered %d messages, %d attachments, and %d metadata records.\n",
			mcapWriter.Statistics.MessageCount,
			mcapWriter.Statistics.AttachmentCount,
			mcapWriter.Statistics.MetadataCount,
		)
	}()

	lexer, err := mcap.NewLexer(r, &mcap.LexerOptions{
		ValidateChunkCRCs: true,
		EmitChunks:        !ops.decodeChunk,
		EmitInvalidChunks: true,
		AttachmentCallback: func(ar *mcap.AttachmentReader) error {
			err = mcapWriter.WriteAttachment(&mcap.Attachment{
				LogTime:    ar.LogTime,
				CreateTime: ar.CreateTime,
				Name:       ar.Name,
				MediaType:  ar.MediaType,
				DataSize:   ar.DataSize,
				Data:       ar.Data(),
			})
			if err != nil {
				return err
			}
			return nil
		},
	})
	if err != nil {
		return err
	}

	buf := make([]byte, 1024)
	var lastChunk *mcap.Chunk
	var lastIndexes []*mcap.MessageIndex
	var recordsCopy []byte

	for {
		token, data, err := lexer.Next(buf)
		if err != nil {
			if token == mcap.TokenInvalidChunk {
				fmt.Fprintf(os.Stderr, "Invalid chunk encountered, skipping: %s\n", err)
				continue
			}
			if lastChunk != nil {
				// Reconstruct message indexes for the last chunk, because it is unclear if the
				// message indexes are complete or not.
				idx, err := utils.UpdateInfoFromChunk(info, lastChunk, nil)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to update info from chunk, skipping: %s\n", err)
				} else {
					err = mcapWriter.WriteChunkWithIndexes(lastChunk, idx)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to write chunk, skipping: %s\n", err)
					}
				}
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			var expected *mcap.ErrTruncatedRecord
			if errors.As(err, &expected) {
				fmt.Fprintln(os.Stderr, expected.Error())
				return nil
			}
			return nil
		}
		if len(data) > len(buf) {
			buf = data
		}

		if token != mcap.TokenMessageIndex {
			if lastChunk != nil {
				lastIndexes, err = utils.UpdateInfoFromChunk(info, lastChunk, lastIndexes)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to update info from chunk, skipping: %s\n", err)
				} else {
					err = mcapWriter.WriteChunkWithIndexes(lastChunk, lastIndexes)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to write chunk, skipping: %s\n", err)
					}
				}
				lastIndexes = nil
				lastChunk = nil
			}
		}

		switch token {
		case mcap.TokenHeader:
			header, err := mcap.ParseHeader(data)
			if err != nil {
				return err
			}
			if err := mcapWriter.WriteHeader(header); err != nil {
				return err
			}
		case mcap.TokenChunk:
			chunk, err := mcap.ParseChunk(data)

			if decodeChunk {
				idx, err := utils.UpdateInfoFromChunk(info, chunk, nil)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to update info from chunk, skipping: %s\n", err)
				} else {
					err = mcapWriter.WriteChunkWithIndexes(chunk, idx)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to write chunk, skipping: %s\n", err)
					}
				}
			} else {
				// copy the records, since it is referenced and the buffer will be reused
				if cap(recordsCopy) < len(chunk.Records) {
					recordsCopy = make([]byte, len(chunk.Records))
				} else {
					recordsCopy = recordsCopy[:len(chunk.Records)]
				}
				copy(recordsCopy, chunk.Records)
				lastChunk = chunk
				lastChunk.Records = recordsCopy

				if err != nil {
					return err
				}
			}
		case mcap.TokenMessageIndex:
			if !decodeChunk {
				if lastChunk == nil {
					return fmt.Errorf("got message index but not chunk before it")
				}
				index, err := mcap.ParseMessageIndex(data)
				if err != nil {
					return err
				}
				lastIndexes = append(lastIndexes, index)
			}
		case mcap.TokenMetadata:
			metadata, err := mcap.ParseMetadata(data)
			if err != nil {
				return err
			}
			if err := mcapWriter.WriteMetadata(metadata); err != nil {
				return err
			}
		case mcap.TokenSchema:
			decodeChunk = true // mcap is not chunked
			schema, err := mcap.ParseSchema(data)
			if err != nil {
				return err
			}
			if err := mcapWriter.WriteSchema(schema); err != nil {
				return err
			}
		case mcap.TokenChannel:
			decodeChunk = true // mcap is not chunked
			channel, err := mcap.ParseChannel(data)
			if err != nil {
				return err
			}
			if err := mcapWriter.WriteChannel(channel); err != nil {
				return err
			}
		case mcap.TokenMessage:
			decodeChunk = true // mcap is not chunked
			message, err := mcap.ParseMessage(data)
			if err != nil {
				return err
			}
			if err := mcapWriter.WriteMessage(message); err != nil {
				return err
			}
		case mcap.TokenDataEnd, mcap.TokenFooter:
			// data section is over, either because the file is over or the summary section starts.
			return nil
		case mcap.TokenError:
			return errors.New("received error token but lexer did not return error on Next")
		}
	}
}

func init() {
	var recoverCmd = &cobra.Command{
		Use:   "recover [file]",
		Short: "Recover data from a potentially corrupt MCAP file",
		Long: `This subcommand reads a potentially corrupt MCAP file and copies data to a new file.

usage:
  mcap recover in.mcap -o out.mcap`,
	}
	output := recoverCmd.PersistentFlags().StringP("output", "o", "", "output filename")
	alwaysDecodeChunk := recoverCmd.PersistentFlags().BoolP(
		"always-decode-chunk",
		"a",
		false,
		"always decode chunks, even if the file is not chunked",
	)
	chunkSize := recoverCmd.PersistentFlags().Int64P("chunk-size", "", 4*1024*1024, "chunk size of output file")
	compression := recoverCmd.PersistentFlags().String(
		"compression",
		"zstd",
		"compression algorithm to use on output file",
	)
	var compressionFormat mcap.CompressionFormat
	switch *compression {
	case CompressionFormatZstd:
		compressionFormat = mcap.CompressionZSTD
	case CompressionFormatLz4:
		compressionFormat = mcap.CompressionLZ4
	case CompressionFormatNone:
	case "":
		compressionFormat = mcap.CompressionNone
	default:
		die(
			"unrecognized compression format '%s': valid options are 'lz4', 'zstd', or 'none'",
			*compression,
		)
	}
	recoverCmd.Run = func(_ *cobra.Command, args []string) {
		var reader io.Reader
		if len(args) == 0 {
			stat, err := os.Stdin.Stat()
			if err != nil {
				die("failed to check stdin state: %s", err)
			}
			if stat.Mode()&os.ModeCharDevice == 0 {
				reader = os.Stdin
			} else {
				die("please supply a file. see --help for usage details.")
			}
		} else {
			closeFile, newReader, err := utils.GetReader(context.Background(), args[0])
			if err != nil {
				die("failed to open source for reading: %s", err)
			}
			defer func() {
				if closeErr := closeFile(); closeErr != nil {
					die("error closing read source: %s", closeErr)
				}
			}()
			reader = newReader
		}

		var writer io.Writer
		if *output == "" {
			if !utils.StdoutRedirected() {
				die(PleaseRedirect)
			}
			writer = os.Stdout
		} else {
			newWriter, err := os.Create(*output)
			if err != nil {
				die("failed to open %s for writing: %s", *output, err)
			}
			defer func() {
				if err := newWriter.Close(); err != nil {
					die("error closing write target: %s", err)
				}
			}()
			writer = newWriter
		}

		err := recoverRun(reader, writer, &recoverOptions{
			decodeChunk: *alwaysDecodeChunk,
			chunkSize:   *chunkSize,
			compression: compressionFormat,
		})
		if err != nil {
			die("failed to recover: %s", err)
		}
	}
	rootCmd.AddCommand(recoverCmd)
}
