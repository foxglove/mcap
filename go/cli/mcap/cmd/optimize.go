package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var (
	optimizeMode   string
	optimizeTopics []string
)

func optimize(r io.Reader, w io.Writer, topics map[string]bool) error {
	lexer, err := mcap.NewLexer(r, &mcap.LexerOptions{
		SkipMagic:   false,
		ValidateCRC: false,
		EmitChunks:  false,
	})
	if err != nil {
		return fmt.Errorf("failed to construct lexer: %w", err)
	}
	writer, err := mcap.NewWriter(w, &mcap.WriterOptions{
		IncludeCRC:  true,
		Chunked:     true,
		ChunkSize:   4 * 1024 * 1024,
		Compression: "zstd",
	})
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}
	attachmentBuffer := bytes.Buffer{}
	attachmentWriter, err := mcap.NewWriter(&attachmentBuffer, &mcap.WriterOptions{
		IncludeCRC:  true,
		Chunked:     true,
		ChunkSize:   4 * 1024 * 1024,
		Compression: "zstd",
	})
	if err != nil {
		return fmt.Errorf("failed to create attachment writer: %w", err)
	}
	schemas := make(map[uint16]*mcap.Schema)
	channels := make(map[uint16]*mcap.Channel)
	buf := make([]byte, 1024)
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

		case mcap.TokenHeader:
			record, err := mcap.ParseHeader(token)
			if err != nil {
				return fmt.Errorf("failed to parse header: %w", err)
			}
			err = writer.WriteHeader(record)
			if err != nil {
				return fmt.Errorf("failed to write header to optimized mcap: %w", err)
			}
			err = attachmentWriter.WriteHeader(record)
			if err != nil {
				return fmt.Errorf("failed to write header to preload attachment: %w", err)
			}
		case mcap.TokenSchema:
			record, err := mcap.ParseSchema(token)
			if err != nil {
				return fmt.Errorf("failed to parse schema: %w", err)
			}
			if _, ok := schemas[record.ID]; !ok {
				err := writer.WriteSchema(record)
				if err != nil {
					return fmt.Errorf("failed to write schema: %w", err)
				}
				schemas[record.ID] = record
			}
		case mcap.TokenChannel:
			record, err := mcap.ParseChannel(token)
			if err != nil {
				return fmt.Errorf("failed to parse channel: %w", err)
			}
			if _, ok := channels[record.ID]; !ok {
				err := writer.WriteChannel(record)
				if err != nil {
					return fmt.Errorf("failed to write channel: %w", err)
				}
				channels[record.ID] = record

				if topics[record.Topic] {
					err := attachmentWriter.WriteSchema(schemas[record.SchemaID])
					if err != nil {
						return fmt.Errorf("failed to write schema to attachment: %w", err)
					}
					err = attachmentWriter.WriteChannel(record)
					if err != nil {
						return fmt.Errorf("failed to write channel to attachment record", err)
					}
				}
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

			if topics[channels[record.ChannelID].Topic] {
				err := attachmentWriter.WriteMessage(record)
				if err != nil {
					return fmt.Errorf("failed to write message to preload attachment: %w", err)
				}
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
		case mcap.TokenAttachment:
			record, err := mcap.ParseAttachment(token)
			if err != nil {
				return fmt.Errorf("failed to parse metadata: %w", err)
			}
			err = writer.WriteAttachment(record)
			if err != nil {
				return fmt.Errorf("failed to write metadata: %w", err)
			}
		default:
			continue
		}
	}
	err = attachmentWriter.Close()
	if err != nil {
		return fmt.Errorf("failed to close attachment writer: %w", err)
	}
	err = writer.WriteAttachment(&mcap.Attachment{
		LogTime:    0,
		CreateTime: 0,
		Name:       "preload_topics",
		Data:       attachmentBuffer.Bytes(),
	})
	if err != nil {
		return fmt.Errorf("failed to write preload_topics attachment: %w", err)
	}
	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close optimized mcap: %w", err)
	}
	return nil
}

var optimizeCmd = &cobra.Command{
	Use:   "optimize [file]",
	Short: "optimize the mcap file for pre-loading",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("supply a file")
		}
		filename := args[0]
		f, err := os.Open(filename)
		if err != nil {
			die("failed to open file: %s", err)
		}

		topics := make(map[string]bool)
		for _, topic := range optimizeTopics {
			topics[topic] = true
		}

		err = optimize(f, os.Stdout, topics)
		if err != nil {
			die("failed to optimize mcap: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(optimizeCmd)
	optimizeCmd.PersistentFlags().StringSliceVarP(&optimizeTopics, "topic", "t", []string{}, "topics to optimize for preloading")
}
