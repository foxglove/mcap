package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/spf13/cobra"
)

// write chunk and indexes to the writer
// if chunk contains new unseen channels, add them to the writer
// if messageIndex is nil, it will be generated from the chunk
func writeChunk(w *mcap.Writer, c *mcap.Chunk, messageIndexes []*mcap.MessageIndex) error {
	containsNewChannel := false
	recreateMessageIndexes := false
	var messageIndexesByChannelID map[uint16]*mcap.MessageIndex

	if messageIndexes == nil {
		recreateMessageIndexes = true
		messageIndexesByChannelID = make(map[uint16]*mcap.MessageIndex)
	} else {
		for _, messageIndex := range messageIndexes {
			if messageIndex.IsEmpty() {
				continue
			}
			if !w.HasChannel(messageIndex.ChannelID) {
				containsNewChannel = true
			}
			w.Statistics.MessageCount += uint64(len(messageIndex.Records))
			w.Statistics.ChannelMessageCounts[messageIndex.ChannelID] += uint64(len(messageIndex.Records))
		}
	}

	if containsNewChannel || recreateMessageIndexes {
		var uncompressedBytes []byte

		switch mcap.CompressionFormat(c.Compression) {
		case mcap.CompressionNone:
			uncompressedBytes = c.Records
		case mcap.CompressionZSTD:
			compressedDataReader := bytes.NewReader(c.Records)
			chunkDataReader, err := zstd.NewReader(compressedDataReader)
			if err != nil {
				return err
			}
			defer chunkDataReader.Close()
			uncompressedBytes, err = io.ReadAll(chunkDataReader)
			if err != nil {
				return err
			}
		case mcap.CompressionLZ4:
			var err error
			compressedDataReader := bytes.NewReader(c.Records)
			chunkDataReader := lz4.NewReader(compressedDataReader)
			uncompressedBytes, err = io.ReadAll(chunkDataReader)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported compression format: %s", c.Compression)
		}

		uncompressedBytesReader := bytes.NewReader(uncompressedBytes)

		lexer, err := mcap.NewLexer(uncompressedBytesReader, &mcap.LexerOptions{
			SkipMagic: true,
		})
		if err != nil {
			return err
		}
		defer lexer.Close()

		msg := make([]byte, 1024)
		for {
			position, err := uncompressedBytesReader.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}
			token, data, err := lexer.Next(msg)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			if len(data) > len(msg) {
				msg = data
			}

			switch token {
			case mcap.TokenSchema:
				schema, err := mcap.ParseSchema(data)
				if err != nil {
					return err
				}
				w.AddSchema(schema)
			case mcap.TokenChannel:
				channel, err := mcap.ParseChannel(data)
				if err != nil {
					return err
				}
				w.AddChannel(channel)
			case mcap.TokenMessage:
				if recreateMessageIndexes {
					m, err := mcap.ParseMessage(data)
					if err != nil {
						return err
					}
					idx, ok := messageIndexesByChannelID[m.ChannelID]
					if !ok {
						idx = &mcap.MessageIndex{
							ChannelID: m.ChannelID,
							Records:   nil,
						}
						messageIndexesByChannelID[m.ChannelID] = idx
					}
					if err != nil {
						return err
					}
					idx.Add(m.LogTime, uint64(position))

					// Also update stats if recreating indexes
					w.Statistics.MessageCount++
					w.Statistics.ChannelMessageCounts[m.ChannelID]++
				}
			}
		}
	}

	if recreateMessageIndexes {
		messageIndexes = make([]*mcap.MessageIndex, 0, len(messageIndexesByChannelID))
		for _, idx := range messageIndexesByChannelID {
			messageIndexes = append(messageIndexes, idx)
		}
	}
	return w.WriteChunkWithIndexes(c, messageIndexes)
}

func recoverRun(
	r io.Reader,
	w io.Writer,
	decodeChunk bool,
) error {
	mcapWriter, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Chunked: true,
	})
	if err != nil {
		return err
	}

	defer func() {
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
		EmitChunks:        true,
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

	for {
		token, data, err := lexer.Next(buf)
		if err != nil {
			if token == mcap.TokenInvalidChunk {
				fmt.Printf("Invalid chunk encountered, skipping: %s\n", err)
				continue
			}
			if lastChunk != nil {
				// Reconstruct message indexes for the last chunk, because it is unclear if the
				// message indexes are complete or not.
				err = writeChunk(mcapWriter, lastChunk, nil)
				if err != nil {
					fmt.Printf("Failed to add channels for last chunk: %s\n", err)
					return err
				}
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			var expected *mcap.ErrTruncatedRecord
			if errors.As(err, &expected) {
				fmt.Println(expected.Error())
				return nil
			}
			return err
		}
		if len(data) > len(buf) {
			buf = data
		}

		if token != mcap.TokenMessageIndex {
			if lastChunk != nil {
				err = writeChunk(mcapWriter, lastChunk, lastIndexes)
				if err != nil {
					fmt.Printf("TokenMessageIndex: Failed to add channels for last chunk: %s\n", err)
					return err
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
				err = writeChunk(mcapWriter, chunk, nil)
				if err != nil {
					fmt.Printf("TokenChunk: Failed to add channels for last chunk: %s\n", err)
					return err
				}
			} else {
				// copy the records, since it is referenced and the buffer will be reused
				recordsCopy := make([]byte, len(chunk.Records))
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
	alwaysDecodeChunk := recoverCmd.PersistentFlags().BoolP("always-decode-chunk", "a", false, "always decode chunks, even if the file is not chunked")
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

		err := recoverRun(reader, writer, *alwaysDecodeChunk)
		if err != nil {
			die("failed to recover: %s", err)
		}
	}
	rootCmd.AddCommand(recoverCmd)
}
