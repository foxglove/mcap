package cmd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/foxglove/mcap/go/libmcap"
	"github.com/spf13/cobra"
)

type mcapDoctor struct {
	reader          io.Reader
	opcodeAndLength []byte

	channels map[uint16]*libmcap.ChannelInfo
}

type RawRecord struct {
	OpCode libmcap.OpCode
	Bytes  []byte
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

func (doctor *mcapDoctor) examineChunk(chunk *libmcap.Chunk) {
	if chunk.Compression != "lz4" && chunk.Compression != "zstd" && chunk.Compression != "" {
		doctor.error("Unsupported compression format: %s", chunk.Compression)
	}

	// create a new record iterator for this set of bytes
	var records = chunk.Records
	var offset uint64 = 0

	var minRecordTime uint64 = math.MaxUint64
	var maxRecordTime uint64 = 0

	for {
		if offset == uint64(len(records)) {
			break
		}
		if offset+9 >= uint64(len(records)) {
			doctor.error("Malformed chunk records")
			break
		}
		opcode := libmcap.OpCode(records[offset])
		offset += 1
		recordLen := binary.LittleEndian.Uint64(records[offset : offset+8])
		offset += 8

		if offset+recordLen >= uint64(len(records)) {
			doctor.error("Not enough bytes remaining in chunk records for record content")
			break
		}

		recordBytes := records[offset : offset+recordLen]
		offset += recordLen

		switch opcode {
		case libmcap.OpChannelInfo:
			channelInfo, err := libmcap.ParseChannelInfo(recordBytes)
			if err != nil {
				doctor.error("Error parsing ChannelInfo: %s", err)
			}

			var msgEncoding = channelInfo.MessageEncoding
			var isCustomMessageEncoding = strings.HasPrefix(msgEncoding, "x-")
			if len(msgEncoding) > 0 && msgEncoding != "proto" && msgEncoding != "ros1" && !isCustomMessageEncoding {
				doctor.error("ChannelInfo.messageEncoding field is not valid: %s. Only a well-known encodings are allowed. Other encodings must use x- prefix", msgEncoding)
			}

			var schemaEncoding = channelInfo.SchemaEncoding
			var isCustomSchemaEncoding = strings.HasPrefix(schemaEncoding, "x-")
			if len(schemaEncoding) > 0 && schemaEncoding != "proto" && schemaEncoding != "ros1msg" && !isCustomSchemaEncoding {
				doctor.error("ChannelInfo.schemaEncoding field is not valid: %s. Only a well-known schemas are allowed. Other schemas must use x- prefix", msgEncoding)
			}

			doctor.channels[channelInfo.ChannelID] = channelInfo
		case libmcap.OpMessage:
			message, err := libmcap.ParseMessage(recordBytes)
			if err != nil {
				doctor.error("Error parsing Message: %s", err)
			}

			channel := doctor.channels[message.ChannelID]
			if channel == nil {
				doctor.error("Got a Message record for channel: %d before a channel info.", message.ChannelID)
			}

			if message.RecordTime < minRecordTime {
				minRecordTime = message.RecordTime
			}

			if message.RecordTime > maxRecordTime {
				maxRecordTime = message.RecordTime
			}

		default:
			doctor.error("Illegal record in chunk: %d", opcode)
		}
	}

	if minRecordTime != chunk.StartTime {
		doctor.error("Chunk.start_time %d does not match the latest message record time %d", chunk.StartTime, minRecordTime)
	}

	if maxRecordTime != chunk.EndTime {
		doctor.error("Chunk.end_time %d does not match the latest message record time %d", chunk.EndTime, maxRecordTime)
	}

	// error if chunk uncompressed size doesn't match
}

func (doctor *mcapDoctor) Examine() {
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
			header, err := libmcap.ParseHeader(rawRecord.Bytes)
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
		case libmcap.OpChannelInfo:
			channelInfo, err := libmcap.ParseChannelInfo(rawRecord.Bytes)
			if err != nil {
				doctor.error("Error parsing ChannelInfo: %s", err)
			}

			var msgEncoding = channelInfo.MessageEncoding
			var isCustomMessageEncoding = strings.HasPrefix(msgEncoding, "x-")
			if len(msgEncoding) > 0 && msgEncoding != "proto" && msgEncoding != "ros1" && !isCustomMessageEncoding {
				doctor.error("ChannelInfo.messageEncoding field is not valid: %s. Only a well-known encodings are allowed. Other encodings must use x- prefix", msgEncoding)
			}

			var schemaEncoding = channelInfo.SchemaEncoding
			var isCustomSchemaEncoding = strings.HasPrefix(schemaEncoding, "x-")
			if len(schemaEncoding) > 0 && schemaEncoding != "proto" && schemaEncoding != "ros1msg" && !isCustomSchemaEncoding {
				doctor.error("ChannelInfo.schemaEncoding field is not valid: %s. Only a well-known schemas are allowed. Other schemas must use x- prefix", msgEncoding)
			}

			doctor.channels[channelInfo.ChannelID] = channelInfo

		case libmcap.OpMessage:
			message, err := libmcap.ParseMessage(rawRecord.Bytes)
			if err != nil {
				doctor.error("Error parsing Message: %s", err)
			}

			channel := doctor.channels[message.ChannelID]
			if channel == nil {
				doctor.error("Got a Message record for channel: %d before a channel info.", message.ChannelID)
			}

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
}

func NewMcapDoctor(reader io.Reader) *mcapDoctor {
	return &mcapDoctor{
		reader:          reader,
		opcodeAndLength: make([]byte, 9),
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
