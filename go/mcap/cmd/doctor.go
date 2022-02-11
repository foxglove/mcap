package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/foxglove/mcap/go/libmcap"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/spf13/cobra"
)

type mcapDoctor struct {
	reader io.Reader

	channels map[uint16]*libmcap.Channel
	schemas  map[uint16]*libmcap.Schema
}

type MessageEncoding string
type SchemaEncoding string

const (
	MessageEncodingProto MessageEncoding = "proto"
	MessageEncodingRos1  MessageEncoding = "ros1"
)

const (
	SchemaEncodingProto   SchemaEncoding = "proto"
	SchemaEncodingRos1Msg SchemaEncoding = "ros1msg"
)

func (doctor *mcapDoctor) warn(format string, v ...interface{}) {
	color.Yellow(format, v...)
}

func (doctor *mcapDoctor) error(format string, v ...interface{}) {
	color.Red(format, v...)
}

func (doctor *mcapDoctor) fatal(v ...interface{}) {
	color.Set(color.FgRed)
	fmt.Println(v...)
	color.Unset()
	os.Exit(1)
}

func (doctor *mcapDoctor) fatalf(format string, v ...interface{}) {
	color.Red(format, v...)
	os.Exit(1)
}

func (doctor *mcapDoctor) examineChunk(chunk *libmcap.Chunk) {
	compressionFormat := libmcap.CompressionFormat(chunk.Compression)
	var uncompressedBytes []byte

	switch compressionFormat {
	case libmcap.CompressionNone:
		uncompressedBytes = chunk.Records
	case libmcap.CompressionZSTD:
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
	case libmcap.CompressionLZ4:
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

	lexer, err := libmcap.NewLexer(uncompressedBytesReader, &libmcap.LexOpts{
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
		case libmcap.TokenSchema:
			schema, err := libmcap.ParseSchema(data)
			if err != nil {
				doctor.error("Failed to parse schema:", err)
			}

			var schemaEncoding = schema.Encoding
			var isCustomSchemaEncoding = strings.HasPrefix(schemaEncoding, "x-")
			if len(schemaEncoding) > 0 && schemaEncoding != string(SchemaEncodingProto) &&
				schemaEncoding != string(SchemaEncodingRos1Msg) && !isCustomSchemaEncoding {
				doctor.error("Schema.encoding field is not valid: %s. Only a well-known schemas are allowed. Other schemas must use x- prefix", schemaEncoding)
			}

			if schema.Encoding == "" && len(schema.Data) > 0 {
				doctor.error("Schema.data field should not be set when Schema.encoding is empty")
			}

			if schema.ID == 0 {
				doctor.error("Schema.ID 0 is reserved. Do not make Schema records with ID 0.")
			}

			doctor.schemas[schema.ID] = schema
		case libmcap.TokenChannel:
			channel, err := libmcap.ParseChannel(data)
			if err != nil {
				doctor.error("Error parsing Channel: %s", err)
			}

			var msgEncoding = channel.MessageEncoding
			var isCustomMessageEncoding = strings.HasPrefix(msgEncoding, "x-")
			if len(msgEncoding) > 0 && msgEncoding != string(MessageEncodingProto) &&
				msgEncoding != string(MessageEncodingRos1) && !isCustomMessageEncoding {
				doctor.error("Channel.messageEncoding field is not valid: %s. Only a well-known encodings are allowed. Other encodings must use x- prefix", msgEncoding)
			}

			doctor.channels[channel.ID] = channel

			if _, ok := doctor.schemas[channel.SchemaID]; !ok {
				doctor.error("Encountered Channel (%d) with unknown Schema (%d)", channel.ID, channel.SchemaID)
			}
		case libmcap.TokenMessage:
			message, err := libmcap.ParseMessage(data)
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
		doctor.error("Chunk.start_time %d does not match the latest message record time %d", chunk.MessageStartTime, minLogTime)
	}

	if maxLogTime != chunk.MessageEndTime {
		doctor.error("Chunk.end_time %d does not match the latest message record time %d", chunk.MessageEndTime, maxLogTime)
	}
}

func (doctor *mcapDoctor) Examine() {
	lexer, err := libmcap.NewLexer(doctor.reader, &libmcap.LexOpts{
		SkipMagic:   false,
		ValidateCRC: true,
		EmitChunks:  true,
	})
	if err != nil {
		doctor.fatal(err)
	}

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
		case libmcap.TokenHeader:
			header, err := libmcap.ParseHeader(data)
			if err != nil {
				doctor.error("Error parsing Header: %s", err)
			}

			if header.Library == "" {
				doctor.warn("Header.library field should be non-empty. Its good to include a library field for reference.")
			}

			var customProfile = strings.HasPrefix(header.Profile, "x-")
			if len(header.Profile) > 0 && header.Profile != "ros1" && header.Profile != "ros2" && !customProfile {
				doctor.error("Header.profile field is not valid: %s. Only a well-known profile is allowed. Other profiles must use x- prefix", header.Profile)
			}
		case libmcap.TokenFooter:
			_, err := libmcap.ParseFooter(data)
			if err != nil {
				doctor.error("Failed to parse footer:", err)
			}
		case libmcap.TokenSchema:
			schema, err := libmcap.ParseSchema(data)
			if err != nil {
				doctor.error("Failed to parse schema:", err)
			}

			var schemaEncoding = schema.Encoding
			var isCustomSchemaEncoding = strings.HasPrefix(schemaEncoding, "x-")
			if len(schemaEncoding) > 0 && schemaEncoding != string(SchemaEncodingProto) &&
				schemaEncoding != string(SchemaEncodingRos1Msg) && !isCustomSchemaEncoding {
				doctor.error("Schema.encoding field is not valid: %s. Only a well-known schemas are allowed. Other schemas must use x- prefix", schemaEncoding)
			}

			if schema.Encoding == "" && len(schema.Data) > 0 {
				doctor.error("Schema.data field should not be set when Schema.encoding is empty")
			}

			if schema.ID == 0 {
				doctor.error("Schema.ID 0 is reserved. Do not make Schema records with ID 0.")
			}

			doctor.schemas[schema.ID] = schema
		case libmcap.TokenChannel:
			channel, err := libmcap.ParseChannel(data)
			if err != nil {
				doctor.error("Error parsing Channel: %s", err)
			}

			var msgEncoding = channel.MessageEncoding
			var isCustomMessageEncoding = strings.HasPrefix(msgEncoding, "x-")
			if len(msgEncoding) > 0 && msgEncoding != string(MessageEncodingProto) && msgEncoding != string(MessageEncodingRos1) && !isCustomMessageEncoding {
				doctor.error("Channel.messageEncoding field is not valid: %s. Only a well-known encodings are allowed. Other encodings must use x- prefix", msgEncoding)
			}

			doctor.channels[channel.ID] = channel

			if _, ok := doctor.schemas[channel.SchemaID]; !ok {
				doctor.error("Encountered Channel (%d) with unknown Schema (%d)", channel.ID, channel.SchemaID)
			}
		case libmcap.TokenMessage:
			message, err := libmcap.ParseMessage(data)
			if err != nil {
				doctor.error("Error parsing Message: %s", err)
			}

			channel := doctor.channels[message.ChannelID]
			if channel == nil {
				doctor.error("Got a Message record for channel: %d before a channel info.", message.ChannelID)
			}
		case libmcap.TokenChunk:
			chunk, err := libmcap.ParseChunk(data)
			if err != nil {
				doctor.error("Error parsing Message: %s", err)
			}

			doctor.examineChunk(chunk)
		case libmcap.TokenMessageIndex:
			_, err := libmcap.ParseMessageIndex(data)
			if err != nil {
				doctor.error("Failed to parse message index:", err)
			}
		case libmcap.TokenChunkIndex:
			_, err := libmcap.ParseChunkIndex(data)
			if err != nil {
				doctor.error("Failed to parse chunk index:", err)
			}
		case libmcap.TokenAttachment:
			_, err := libmcap.ParseAttachment(data)
			if err != nil {
				doctor.error("Failed to parse attachment:", err)
			}
		case libmcap.TokenAttachmentIndex:
			_, err := libmcap.ParseAttachmentIndex(data)
			if err != nil {
				doctor.error("Failed to parse attachment index:", err)
			}
		case libmcap.TokenStatistics:
			_, err := libmcap.ParseStatistics(data)
			if err != nil {
				doctor.error("Failed to parse statistics:", err)
			}
		case libmcap.TokenMetadata:
			_, err := libmcap.ParseMetadata(data)
			if err != nil {
				doctor.error("Failed to parse metadata:", err)
			}
		case libmcap.TokenMetadataIndex:
			_, err := libmcap.ParseMetadataIndex(data)
			if err != nil {
				doctor.error("Failed to parse metadata index:", err)
			}
		case libmcap.TokenSummaryOffset:
			_, err := libmcap.ParseSummaryOffset(data)
			if err != nil {
				doctor.error("Failed to parse summary offset:", err)
			}
		case libmcap.TokenDataEnd:
			_, err := libmcap.ParseDataEnd(data)
			if err != nil {
				doctor.error("Failed to parse data end:", err)
			}
		case libmcap.TokenError:
			// this is the value of the tokenType when there is an error
			// from the lexer, which we caught at the top.
			doctor.fatalf("Failed to parse:", err)
		}
	}
}

func newMcapDoctor(reader io.Reader) *mcapDoctor {
	return &mcapDoctor{
		reader:   reader,
		channels: make(map[uint16]*libmcap.Channel),
		schemas:  make(map[uint16]*libmcap.Schema),
	}
}

func main(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println("An mcap file argument is required.")
		os.Exit(1)
	}

	fmt.Printf("Examining %s\n", args[0])
	file, err := os.Open(args[0])
	if err != nil {
		fmt.Print(err)
		os.Exit(1)
	}

	doctor := newMcapDoctor(file)
	doctor.Examine()
}

var doctorCommand = &cobra.Command{
	Use:   "doctor <file>",
	Short: "Check an mcap file structure",
	Run:   main,
}

func init() {
	rootCmd.AddCommand(doctorCommand)
}
