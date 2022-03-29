package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/spf13/cobra"
)

type mcapDoctor struct {
	reader io.Reader

	channels map[uint16]*mcap.Channel
	schemas  map[uint16]*mcap.Schema
}

var (
	supportedMessageEncodings = map[string]bool{
		"":         true,
		"ros1":     true,
		"cdr":      true,
		"protobuf": true,
		"cbor":     true,
		"json":     true,
	}
	supportedSchemaEncodings = map[string]bool{
		"":           true,
		"protobuf":   true,
		"ros1msg":    true,
		"ros2msg":    true,
		"jsonschema": true,
	}
)

func (doctor *mcapDoctor) warn(format string, v ...any) {
	color.Yellow(format, v...)
}

func (doctor *mcapDoctor) error(format string, v ...any) {
	color.Red(format, v...)
}

func (doctor *mcapDoctor) fatal(v ...any) {
	color.Set(color.FgRed)
	fmt.Println(v...)
	color.Unset()
	os.Exit(1)
}

func (doctor *mcapDoctor) fatalf(format string, v ...any) {
	color.Red(format, v...)
	os.Exit(1)
}

func (doctor *mcapDoctor) examineChunk(chunk *mcap.Chunk) {
	compressionFormat := mcap.CompressionFormat(chunk.Compression)
	var uncompressedBytes []byte

	switch compressionFormat {
	case mcap.CompressionNone:
		uncompressedBytes = chunk.Records
	case mcap.CompressionZSTD:
		compressedDataReader := bytes.NewReader(chunk.Records)
		chunkDataReader, err := zstd.NewReader(compressedDataReader)
		if err != nil {
			doctor.error("Could not make zstd decoder: %s", err)
			return
		}
		uncompressedBytes, err = io.ReadAll(chunkDataReader)
		if err != nil {
			doctor.error("Could not decompress: %s", err)
			return
		}
	case mcap.CompressionLZ4:
		var err error
		compressedDataReader := bytes.NewReader(chunk.Records)
		chunkDataReader := lz4.NewReader(compressedDataReader)
		uncompressedBytes, err = io.ReadAll(chunkDataReader)
		if err != nil {
			doctor.error("Could not decompress: %s", err)
			return
		}
	default:
		doctor.error("Unsupported compression format: %s", chunk.Compression)
		return
	}

	if uint64(len(uncompressedBytes)) != chunk.UncompressedSize {
		doctor.error("Uncompressed chunk data size != Chunk.uncompressed_size")
		return
	}

	if chunk.UncompressedCRC != 0 {
		crc := crc32.ChecksumIEEE(uncompressedBytes)
		if crc != chunk.UncompressedCRC {
			doctor.error("invalid CRC: %x != %x", crc, chunk.UncompressedCRC)
			return
		}
	}

	uncompressedBytesReader := bytes.NewReader(uncompressedBytes)

	lexer, err := mcap.NewLexer(uncompressedBytesReader, &mcap.LexerOptions{
		SkipMagic:   true,
		ValidateCRC: true,
		EmitChunks:  true,
	})
	if err != nil {
		doctor.error("Failed to make lexer for chunk bytes", err)
		return
	}

	var minLogTime uint64 = math.MaxUint64
	var maxLogTime uint64

	msg := make([]byte, 1024)
	for {
		tokenType, data, err := lexer.Next(msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Fatal("Failed to read token:", err)
		}
		if len(data) > len(msg) {
			msg = data
		}

		if len(data) > len(msg) {
			msg = data
		}
		switch tokenType {
		case mcap.TokenSchema:
			schema, err := mcap.ParseSchema(data)
			if err != nil {
				doctor.error("Failed to parse schema:", err)
			}

			var schemaEncoding = schema.Encoding
			var isCustomSchemaEncoding = strings.HasPrefix(schemaEncoding, "x-")
			if _, ok := supportedSchemaEncodings[schemaEncoding]; !ok && !isCustomSchemaEncoding {
				doctor.error(`
					Schema.encoding field is not valid: %s. Only a well-known schemas are allowed.
					Other schemas must use x- prefix`, schemaEncoding)
			}
			if schema.Encoding == "" && len(schema.Data) > 0 {
				doctor.error("Schema.data field should not be set when Schema.encoding is empty")
			}

			if schema.ID == 0 {
				doctor.error("Schema.ID 0 is reserved. Do not make Schema records with ID 0.")
			}

			doctor.schemas[schema.ID] = schema
		case mcap.TokenChannel:
			channel, err := mcap.ParseChannel(data)
			if err != nil {
				doctor.error("Error parsing Channel: %s", err)
			}

			var msgEncoding = channel.MessageEncoding
			var isCustomMessageEncoding = strings.HasPrefix(msgEncoding, "x-")
			if _, ok := supportedMessageEncodings[msgEncoding]; !ok && !isCustomMessageEncoding {
				doctor.error(`Channel.messageEncoding field is not valid: %s.
					Only a well-known encodings are allowed. Other encodings must use x- prefix`, msgEncoding)
			}
			doctor.channels[channel.ID] = channel
			if _, ok := doctor.schemas[channel.SchemaID]; !ok {
				doctor.error("Encountered Channel (%d) with unknown Schema (%d)", channel.ID, channel.SchemaID)
			}
		case mcap.TokenMessage:
			message, err := mcap.ParseMessage(data)
			if err != nil {
				doctor.error("Error parsing Message: %s", err)
			}

			channel := doctor.channels[message.ChannelID]
			if channel == nil {
				doctor.error("Got a Message record for channel: %d before a channel info.", message.ChannelID)
			}

			if message.LogTime < minLogTime {
				minLogTime = message.LogTime
			}

			if message.LogTime > maxLogTime {
				maxLogTime = message.LogTime
			}

		default:
			doctor.error("Illegal record in chunk: %d", tokenType)
		}
	}

	if minLogTime != chunk.MessageStartTime {
		doctor.error("Chunk.message_start_time %d does not match the earliest message log time %d",
			chunk.MessageStartTime, minLogTime)
	}

	if maxLogTime != chunk.MessageEndTime {
		doctor.error("Chunk.message_end_time %d does not match the latest message log time %d",
			chunk.MessageEndTime, maxLogTime)
	}
}

func (doctor *mcapDoctor) Examine() {
	lexer, err := mcap.NewLexer(doctor.reader, &mcap.LexerOptions{
		SkipMagic:   false,
		ValidateCRC: true,
		EmitChunks:  true,
	})
	if err != nil {
		doctor.fatal(err)
	}

	var lastMessageTime uint64
	msg := make([]byte, 1024)
	for {
		tokenType, data, err := lexer.Next(msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Fatal("Failed to read token:", err)
		}
		if len(data) > len(msg) {
			msg = data
		}
		switch tokenType {
		case mcap.TokenHeader:
			header, err := mcap.ParseHeader(data)
			if err != nil {
				doctor.error("Error parsing Header: %s", err)
			}

			if header.Library == "" {
				doctor.warn("Header.library field should be non-empty. Its good to include a library field for reference.")
			}

			var customProfile = strings.HasPrefix(header.Profile, "x-")
			if len(header.Profile) > 0 && header.Profile != "ros1" && header.Profile != "ros2" && !customProfile {
				doctor.error(`Header.profile field is not valid: %s.
					Only a well-known profile is allowed. Other profiles must use x- prefix`, header.Profile)
			}
		case mcap.TokenFooter:
			_, err := mcap.ParseFooter(data)
			if err != nil {
				doctor.error("Failed to parse footer:", err)
			}
		case mcap.TokenSchema:
			schema, err := mcap.ParseSchema(data)
			if err != nil {
				doctor.error("Failed to parse schema:", err)
			}

			var schemaEncoding = schema.Encoding
			var isCustomSchemaEncoding = strings.HasPrefix(schemaEncoding, "x-")
			if _, ok := supportedSchemaEncodings[schemaEncoding]; !ok && !isCustomSchemaEncoding {
				doctor.error(`Schema.encoding field is not valid: %s.
					Only a well-known schemas are allowed. Other schemas must use x- prefix`, schemaEncoding)
			}
			if schema.Encoding == "" && len(schema.Data) > 0 {
				doctor.error("Schema.data field should not be set when Schema.encoding is empty")
			}

			if schema.ID == 0 {
				doctor.error("Schema.ID 0 is reserved. Do not make Schema records with ID 0.")
			}

			doctor.schemas[schema.ID] = schema
		case mcap.TokenChannel:
			channel, err := mcap.ParseChannel(data)
			if err != nil {
				doctor.error("Error parsing Channel: %s", err)
			}

			var msgEncoding = channel.MessageEncoding
			var isCustomMessageEncoding = strings.HasPrefix(msgEncoding, "x-")
			if _, ok := supportedMessageEncodings[msgEncoding]; !ok && !isCustomMessageEncoding {
				doctor.error(`Channel.messageEncoding field is not valid: %s.
					Only a well-known encodings are allowed. Other encodings must use x- prefix`, msgEncoding)
			}

			doctor.channels[channel.ID] = channel

			if _, ok := doctor.schemas[channel.SchemaID]; !ok {
				doctor.error("Encountered Channel (%d) with unknown Schema (%d)", channel.ID, channel.SchemaID)
			}
		case mcap.TokenMessage:
			message, err := mcap.ParseMessage(data)
			if err != nil {
				doctor.error("Error parsing Message: %s", err)
			}
			channel := doctor.channels[message.ChannelID]
			if channel == nil {
				doctor.error("Got a Message record for channel: %d before a channel info.", message.ChannelID)
			}
			if message.LogTime < lastMessageTime {
				doctor.error("Message.log_time %d on %s is less than the previous message record time %d",
					message.LogTime, channel.Topic, lastMessageTime)
			}
			lastMessageTime = message.LogTime
		case mcap.TokenChunk:
			chunk, err := mcap.ParseChunk(data)
			if err != nil {
				doctor.error("Error parsing Message: %s", err)
			}

			doctor.examineChunk(chunk)
		case mcap.TokenMessageIndex:
			_, err := mcap.ParseMessageIndex(data)
			if err != nil {
				doctor.error("Failed to parse message index:", err)
			}
		case mcap.TokenChunkIndex:
			_, err := mcap.ParseChunkIndex(data)
			if err != nil {
				doctor.error("Failed to parse chunk index:", err)
			}
		case mcap.TokenAttachment:
			_, err := mcap.ParseAttachment(data)
			if err != nil {
				doctor.error("Failed to parse attachment:", err)
			}
		case mcap.TokenAttachmentIndex:
			_, err := mcap.ParseAttachmentIndex(data)
			if err != nil {
				doctor.error("Failed to parse attachment index:", err)
			}
		case mcap.TokenStatistics:
			_, err := mcap.ParseStatistics(data)
			if err != nil {
				doctor.error("Failed to parse statistics:", err)
			}
		case mcap.TokenMetadata:
			_, err := mcap.ParseMetadata(data)
			if err != nil {
				doctor.error("Failed to parse metadata:", err)
			}
		case mcap.TokenMetadataIndex:
			_, err := mcap.ParseMetadataIndex(data)
			if err != nil {
				doctor.error("Failed to parse metadata index:", err)
			}
		case mcap.TokenSummaryOffset:
			_, err := mcap.ParseSummaryOffset(data)
			if err != nil {
				doctor.error("Failed to parse summary offset:", err)
			}
		case mcap.TokenDataEnd:
			_, err := mcap.ParseDataEnd(data)
			if err != nil {
				doctor.error("Failed to parse data end:", err)
			}
		case mcap.TokenError:
			// this is the value of the tokenType when there is an error
			// from the lexer, which we caught at the top.
			doctor.fatalf("Failed to parse:", err)
		}
	}
}

func newMcapDoctor(reader io.Reader) *mcapDoctor {
	return &mcapDoctor{
		reader:   reader,
		channels: make(map[uint16]*mcap.Channel),
		schemas:  make(map[uint16]*mcap.Schema),
	}
}

func main(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	if len(args) != 1 {
		fmt.Println("An mcap file argument is required.")
		os.Exit(1)
	}
	filename := args[0]
	err := utils.WithReader(ctx, filename, func(remote bool, rs io.ReadSeeker) error {
		doctor := newMcapDoctor(rs)
		if remote {
			doctor.warn("Will read full remote file")
		}
		fmt.Printf("Examining %s\n", args[0])
		doctor.Examine()
		return nil
	})
	if err != nil {
		log.Fatalf("Doctor command failed: %s", err)
	}
}

var doctorCommand = &cobra.Command{
	Use:   "doctor <file>",
	Short: "Check an mcap file structure",
	Run:   main,
}

func init() {
	rootCmd.AddCommand(doctorCommand)
}
