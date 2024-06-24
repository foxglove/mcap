package cmd

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/klauspost/compress/zstd"
	"github.com/olekukonko/tablewriter"
	"github.com/pierrec/lz4/v4"
	"github.com/spf13/cobra"
)

type usage struct {
	reader io.ReadSeeker

	channels map[uint16]*mcap.Channel

	// total message size of all messages
	totalMessageSize uint64

	// total message size by topic name
	topicMessageSize map[string]uint64

	totalSize uint64

	// record kind to size
	recordKindSize map[string]uint64
}

func newUsage(reader io.ReadSeeker) *usage {
	return &usage{
		reader:           reader,
		channels:         make(map[uint16]*mcap.Channel),
		topicMessageSize: make(map[string]uint64),
		recordKindSize:   make(map[string]uint64),
	}
}

func (instance *usage) processChunk(chunk *mcap.Chunk) error {
	compressionFormat := mcap.CompressionFormat(chunk.Compression)
	var uncompressedBytes []byte

	switch compressionFormat {
	case mcap.CompressionNone:
		uncompressedBytes = chunk.Records
	case mcap.CompressionZSTD:
		compressedDataReader := bytes.NewReader(chunk.Records)
		chunkDataReader, err := zstd.NewReader(compressedDataReader)
		if err != nil {
			return fmt.Errorf("could not make zstd decoder: %w", err)
		}
		uncompressedBytes, err = io.ReadAll(chunkDataReader)
		if err != nil {
			return fmt.Errorf("could not decompress: %w", err)
		}
	case mcap.CompressionLZ4:
		var err error
		compressedDataReader := bytes.NewReader(chunk.Records)
		chunkDataReader := lz4.NewReader(compressedDataReader)
		uncompressedBytes, err = io.ReadAll(chunkDataReader)
		if err != nil {
			return fmt.Errorf("could not decompress: %w", err)
		}
	default:
		return fmt.Errorf("unsupported compression format: %s", chunk.Compression)
	}

	if uint64(len(uncompressedBytes)) != chunk.UncompressedSize {
		return fmt.Errorf("uncompressed chunk data size != Chunk.uncompressed_size")
	}

	if chunk.UncompressedCRC != 0 {
		crc := crc32.ChecksumIEEE(uncompressedBytes)
		if crc != chunk.UncompressedCRC {
			return fmt.Errorf("invalid CRC: %x != %x", crc, chunk.UncompressedCRC)
		}
	}

	uncompressedBytesReader := bytes.NewReader(uncompressedBytes)

	lexer, err := mcap.NewLexer(uncompressedBytesReader, &mcap.LexerOptions{
		SkipMagic:         true,
		ValidateChunkCRCs: true,
		EmitChunks:        true,
	})
	if err != nil {
		return fmt.Errorf("failed to make lexer for chunk bytes: %s", err)
	}
	defer lexer.Close()

	msg := make([]byte, 1024)
	for {
		tokenType, data, err := lexer.Next(msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read next token: %s", err)
		}
		if len(data) > len(msg) {
			msg = data
		}

		switch tokenType {
		case mcap.TokenChannel:
			channel, err := mcap.ParseChannel(data)
			if err != nil {
				return fmt.Errorf("Error parsing Channel: %s", err)
			}

			instance.channels[channel.ID] = channel
		case mcap.TokenMessage:
			message, err := mcap.ParseMessage(data)
			if err != nil {
				return fmt.Errorf("Error parsing Message: %s", err)
			}

			channel := instance.channels[message.ChannelID]
			if channel == nil {
				return fmt.Errorf("got a Message record for unknown channel: %d", message.ChannelID)
			}

			messageSize := uint64(len(message.Data))

			instance.totalMessageSize += messageSize
			instance.topicMessageSize[channel.Topic] += messageSize
		}
	}

	return nil
}

func (instance *usage) RunDu() error {
	lexer, err := mcap.NewLexer(instance.reader, &mcap.LexerOptions{
		SkipMagic:         false,
		ValidateChunkCRCs: true,
		EmitChunks:        true,
	})
	if err != nil {
		return err
	}
	defer lexer.Close()

	msg := make([]byte, 1024)
	for {
		tokenType, data, err := lexer.Next(msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("failed to read next token: %s", err)
		}
		if len(data) > len(msg) {
			msg = data
		}

		instance.totalSize += uint64(len(data))
		instance.recordKindSize[tokenType.String()] += uint64(len(data))

		switch tokenType {
		case mcap.TokenChannel:
			channel, err := mcap.ParseChannel(data)
			if err != nil {
				return fmt.Errorf("error parsing Channel: %s", err)
			}

			instance.channels[channel.ID] = channel
		case mcap.TokenMessage:
			message, err := mcap.ParseMessage(data)
			if err != nil {
				return fmt.Errorf("error parsing Message: %s", err)
			}
			channel := instance.channels[message.ChannelID]
			if channel == nil {
				return fmt.Errorf("got a Message record for unknown channel: %d", message.ChannelID)
			}

			messageSize := uint64(len(message.Data))

			instance.totalMessageSize += messageSize
			instance.topicMessageSize[channel.Topic] += messageSize
		case mcap.TokenChunk:
			chunk, err := mcap.ParseChunk(data)
			if err != nil {
				return fmt.Errorf("error parsing Message: %s", err)
			}
			err = instance.processChunk(chunk)
			if err != nil {
				return err
			}
		}
	}

	{
		rows := [][]string{}

		for recordKind, size := range instance.recordKindSize {
			row := []string{
				recordKind, fmt.Sprintf("%d", size),
				fmt.Sprintf("%f", float32(size)/float32(instance.totalSize)*100.0),
			}

			rows = append(rows, row)
		}

		printTable(os.Stdout, rows, []string{
			"record kind", "sum bytes", "% of total file bytes",
		})
	}

	fmt.Println()

	{
		rows := [][]string{}

		for topic, topicSize := range instance.topicMessageSize {
			row := []string{
				topic, fmt.Sprintf("%d", topicSize),
				fmt.Sprintf("%f", float32(topicSize)/float32(instance.totalMessageSize)*100.0),
			}

			rows = append(rows, row)
		}

		printTable(os.Stdout, rows, []string{
			"topic", "sum bytes", "% of total message bytes",
		})
	}

	return nil
}

func printTable(w io.Writer, rows [][]string, header []string) {
	buf := &bytes.Buffer{}
	tw := tablewriter.NewWriter(buf)
	tw.SetBorder(false)
	tw.SetAutoWrapText(false)
	tw.SetAlignment(tablewriter.ALIGN_DEFAULT)
	tw.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	tw.SetHeader(header)
	tw.AppendBulk(rows)
	tw.Render()
	// This tablewriter puts a leading space on the lines for some reason, so
	// remove it.
	scanner := bufio.NewScanner(buf)
	for scanner.Scan() {
		fmt.Fprintln(w, strings.TrimLeft(scanner.Text(), " "))
	}
}

var duCmd = &cobra.Command{
	Use:   "du <file>",
	Short: "Report space usage within an MCAP file",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		if len(args) != 1 {
			die("An MCAP file argument is required.")
		}
		filename := args[0]
		err := utils.WithReader(ctx, filename, func(remote bool, rs io.ReadSeeker) error {
			usage := newUsage(rs)
			return usage.RunDu()
		})
		if err != nil {
			die("du command failed: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(duCmd)
}
