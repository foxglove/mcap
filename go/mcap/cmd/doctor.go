package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/foxglove/mcap/go/libmcap"
	"github.com/spf13/cobra"
)

type mcapDoctor struct {
	reader io.Reader

	channels map[uint16]*libmcap.Channel
	schemas  map[uint16]*libmcap.Schema
}

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

/*
func (doctor *mcapDoctor) consumeMagic() {
	magicBytes := make([]byte, len(libmcap.Magic))

	bytesRead, err := io.ReadFull(doctor.reader, magicBytes)
	if err != nil {
		doctor.fatal(err)
	}
	if bytesRead != len(libmcap.Magic) {
		doctor.fatal("Initial MAGIC bytes missing")
	}
	if !bytes.Equal(magicBytes, libmcap.Magic) {
		doctor.fatal("Invalid MAGIC")
	}
}
*/

/*
func (doctor *mcapDoctor) nextRecord() *RawRecord {
	bytesRead, err := doctor.reader.Read(doctor.opcodeAndLength)
	if err != nil {
		doctor.fatal(err)
	}
	if bytesRead != 9 {
		doctor.fatal("No more bytes to read. Expecting Record Opcode byte and Record Length.")
	}

	opcode := libmcap.OpCode(doctor.opcodeAndLength[0])
	recordLen := binary.LittleEndian.Uint64(doctor.opcodeAndLength[1:9])

	recordBytes := make([]byte, recordLen)
	bytesRead, err = doctor.reader.Read(recordBytes)
	if err != nil {
		doctor.fatal(err)
	}
	if uint64(bytesRead) != recordLen {
		doctor.fatalf("Could not read record payload. Wanted %d bytes, got %d.")
	}

	return &RawRecord{
		OpCode: opcode,
		Bytes:  recordBytes,
	}
}
*/

func (doctor *mcapDoctor) examineChunk(chunk *libmcap.Chunk) {
	if chunk.Compression != "lz4" && chunk.Compression != "zstd" && chunk.Compression != "" {
		doctor.error("Unsupported compression format: %s", chunk.Compression)
	}

	byteReader := bytes.NewReader(chunk.Records)
	lexer, err := libmcap.NewLexer(byteReader, &libmcap.LexOpts{
		SkipMagic:   true,
		ValidateCRC: true,
		EmitChunks:  true,
	})
	if err != nil {
		doctor.error("Failed to make lexer for chunk bytes", err)
		return
	}

	var minLogTime uint64 = math.MaxUint64
	var maxLogTime uint64 = 0

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
			if len(schemaEncoding) > 0 && schemaEncoding != "proto" && schemaEncoding != "ros1msg" && !isCustomSchemaEncoding {
				doctor.error("Schema.encoding field is not valid: %s. Only a well-known schemas are allowed. Other schemas must use x- prefix", schemaEncoding)
			}

			if len(schema.Encoding) == 0 && len(schema.Data) > 0 {
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
			if len(msgEncoding) > 0 && msgEncoding != "proto" && msgEncoding != "ros1" && !isCustomMessageEncoding {
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

			if len(header.Library) == 0 {
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
			if len(schemaEncoding) > 0 && schemaEncoding != "proto" && schemaEncoding != "ros1msg" && !isCustomSchemaEncoding {
				doctor.error("Schema.encoding field is not valid: %s. Only a well-known schemas are allowed. Other schemas must use x- prefix", schemaEncoding)
			}

			if len(schema.Encoding) == 0 && len(schema.Data) > 0 {
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
			if len(msgEncoding) > 0 && msgEncoding != "proto" && msgEncoding != "ros1" && !isCustomMessageEncoding {
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

	/*
		doctor.consumeMagic()

		var previousOpcode *libmcap.OpCode = nil
		for {
			var rawRecord = doctor.nextRecord()

			if previousOpcode == nil && rawRecord.OpCode != libmcap.OpHeader {
				doctor.error("First record MUST be a header")
			}

			// fixme - if there's another rawRecord and previousOpcode is footer we've got stuff after the footer

			switch rawRecord.OpCode {
			case libmcap.OpHeader:

			case libmcap.OpChannel:

			case libmcap.OpMessage:

			case libmcap.OpChunk:
				chunk, err := libmcap.ParseChunk(rawRecord.Bytes)
				if err != nil {
					doctor.error("Error parsing Message: %s", err)
				}

				doctor.examineChunk(chunk)

			case libmcap.OpMessageIndex:
				if previousOpcode == nil || (*previousOpcode != libmcap.OpChunk && *previousOpcode != libmcap.OpMessageIndex) {
					doctor.error("MessageIndex records can only follow Chunk records or other Message Index records")
				}
				// validate message index points to valid locations

			case libmcap.OpChunkIndex:
				// error if values point to invalid locations

			case libmcap.OpSummaryOffset:
				// error if opcode is invalid
				// error if values point to invalid locations

			case libmcap.OpAttachment:

			case libmcap.OpMetadata:

			case libmcap.OpMetadataIndex:

			case libmcap.OpFooter:
				// error if values point to invalid locations

			default:
				doctor.error("Unknown record: %d", rawRecord.OpCode)
			}

			previousOpcode = &rawRecord.OpCode
		}

		doctor.consumeMagic()
	*/
}

func NewMcapDoctor(reader io.Reader) *mcapDoctor {
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

	doctor := NewMcapDoctor(file)
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
