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

func recoverFastRun(
	r io.Reader,
	w io.Writer,
) error {

	mcapWriter, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Chunked: true,
	})
	if err != nil {
		return err
	}

	var numMessages, numAttachments, numMetadata int

	defer func() {
		err := mcapWriter.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to close mcap writer: %v\n", err)
			return
		}
		fmt.Fprintf(
			os.Stderr,
			"Recovered %d messages, %d attachments, and %d metadata records.\n",
			numMessages,
			numAttachments,
			numMetadata,
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
			numAttachments++
			return nil
		},
	})
	if err != nil {
		return err
	}

	buf := make([]byte, 1024)
	var lastChunk *mcap.Chunk
	var lastIndexes []*mcap.MessageIndex

	defer func() {
		if lastChunk != nil {
			err = mcapWriter.WriteChunkWithIndexes(lastChunk, lastIndexes)
			if err != nil {
				return
			}
			lastIndexes = nil
			lastChunk = nil
		}
	}()

	for {
		token, data, err := lexer.Next(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			var expected *mcap.ErrTruncatedRecord
			if errors.As(err, &expected) {
				fmt.Println(expected.Error())
				return nil
			}
			if token == mcap.TokenInvalidChunk {
				fmt.Printf("Invalid chunk encountered, skipping: %s\n", err)
				continue
			}
			return err
		}
		if len(data) > len(buf) {
			buf = data
		}

		if token != mcap.TokenMessageIndex {
			if lastChunk != nil {
				err = mcapWriter.WriteChunkWithIndexes(lastChunk, lastIndexes)
				if err != nil {
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
			lastChunk, err = mcap.ParseChunk(data)
			// copy the records, since it is referenced and the buffer will be reused
			recordsCopy := make([]byte, len(lastChunk.Records))
			copy(recordsCopy, lastChunk.Records)
			lastChunk.Records = recordsCopy

			if err != nil {
				return err
			}
		case mcap.TokenMessageIndex:
			if lastChunk == nil {
				return fmt.Errorf("got message index but not chunk before it")
			}
			index, err := mcap.ParseMessageIndex(data)
			if err != nil {
				return err
			}
			numMessages += len(index.Records)
			lastIndexes = append(lastIndexes, index)
		case mcap.TokenMetadata:
			metadata, err := mcap.ParseMetadata(data)
			if err != nil {
				return err
			}
			if err := mcapWriter.WriteMetadata(metadata); err != nil {
				return err
			}
			numMetadata++
		case mcap.TokenDataEnd, mcap.TokenFooter:
			// data section is over, either because the file is over or the summary section starts.
			return nil
		case mcap.TokenError:
			return errors.New("received error token but lexer did not return error on Next")
		}
	}
}

func init() {
	var recoverFast = &cobra.Command{
		Use:   "recover-fast [file]",
		Short: "Recover data from a potentially corrupt MCAP file without decompressing",
		Long: `This subcommand reads a potentially corrupt MCAP file and copies data to a new file.
It does not decompress the chunks, so it is much faster than the regular recover command.

	usage:
	mcap recover in.mcap -o out.mcap`,
	}
	output := recoverFast.PersistentFlags().StringP("output", "o", "", "output filename")
	recoverFast.Run = func(_ *cobra.Command, args []string) {
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

		err := recoverFastRun(reader, writer)
		if err != nil {
			die("failed to recover: %s", err)
		}

	}
	rootCmd.AddCommand(recoverFast)
}
