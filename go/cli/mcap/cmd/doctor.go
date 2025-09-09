package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"reflect"
	"unicode/utf8"

	"github.com/fatih/color"
	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/spf13/cobra"
)

var (
	verbose            bool
	strictMessageOrder bool
)

type mcapDoctor struct {
	reader io.ReadSeeker

	channelsInDataSection              map[uint16]*mcap.Channel
	schemasInDataSection               map[uint16]*mcap.Schema
	channelsReferencedInChunksByOffset map[uint64][]uint16
	channelIDsInSummarySection         map[uint16]bool
	schemaIDsInSummarySection          map[uint16]bool

	// Map from chunk offset to chunk index
	chunkIndexes map[uint64]*mcap.ChunkIndex

	inSummarySection bool

	messageCount uint64
	minLogTime   uint64
	maxLogTime   uint64
	statistics   *mcap.Statistics

	diagnosis Diagnosis
}

func (doctor *mcapDoctor) warn(format string, v ...any) {
	color.Yellow(format, v...)
	doctor.diagnosis.Warnings = append(doctor.diagnosis.Warnings, fmt.Sprintf(format, v...))
}

func (doctor *mcapDoctor) error(format string, v ...any) {
	color.Red(format, v...)
	doctor.diagnosis.Errors = append(doctor.diagnosis.Errors, fmt.Sprintf(format, v...))
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

func (doctor *mcapDoctor) examineSchema(schema *mcap.Schema) {
	if !utf8.ValidString(schema.Encoding) {
		doctor.error("schema with ID (%d) encoding is not valid UTF-8: %q", schema.ID, schema.Encoding)
	}
	if !utf8.ValidString(schema.Name) {
		doctor.error("schema with ID (%d) name is not valid UTF-8: %q", schema.ID, schema.Name)
	}
	if schema.Encoding == "" {
		if len(schema.Data) == 0 {
			doctor.warn("Schema with ID: %d, Name: %q has empty Encoding and Data fields", schema.ID, schema.Name)
		} else {
			doctor.error("Schema with ID: %d has empty Encoding but Data contains: %q", schema.ID, string(schema.Data))
		}
	}

	if schema.ID == 0 {
		doctor.error("Schema.ID 0 is reserved. Do not make Schema records with ID 0.")
	}
	previous := doctor.schemasInDataSection[schema.ID]
	if previous != nil {
		if schema.Name != previous.Name {
			doctor.error("Two schema records with same ID %d but different names (%q != %q)",
				schema.ID,
				schema.Name,
				previous.Name,
			)
		}
		if schema.Encoding != previous.Encoding {
			doctor.error("Two schema records with same ID %d but different encodings (%q != %q)",
				schema.ID,
				schema.Encoding,
				previous.Encoding,
			)
		}
		if !bytes.Equal(schema.Data, previous.Data) {
			doctor.error("Two schema records with different data present with same ID %d", schema.ID)
		}
	}
	if doctor.inSummarySection {
		if previous == nil {
			doctor.error("Schema with id %d in summary section does not exist in data section", schema.ID)
		}
		doctor.schemaIDsInSummarySection[schema.ID] = true
	} else {
		if previous != nil {
			doctor.warn("Duplicate schema records in data section with ID %d", schema.ID)
		}
		doctor.schemasInDataSection[schema.ID] = schema
	}
}

func (doctor *mcapDoctor) examineChannel(channel *mcap.Channel) {
	if !utf8.ValidString(channel.Topic) {
		doctor.error("channel with ID (%d) topic is not valid UTF-8: %q", channel.ID, channel.Topic)
	}
	if !utf8.ValidString(channel.MessageEncoding) {
		doctor.error("channel with ID (%d) message encoding is not valid UTF-8: %q", channel.ID, channel.MessageEncoding)
	}
	for key, value := range channel.Metadata {
		if !utf8.ValidString(key) {
			doctor.error("channel with ID (%d) metadata key is not valid UTF-8: %q", channel.ID, key)
		}
		if !utf8.ValidString(value) {
			doctor.error("channel with ID (%d) metadata key is not valid UTF-8: %q", channel.ID, value)
		}
	}
	previous := doctor.channelsInDataSection[channel.ID]
	if previous != nil {
		if channel.SchemaID != previous.SchemaID {
			doctor.error("Two channel records with same ID %d but different schema IDs (%d != %d)",
				channel.ID,
				channel.SchemaID,
				previous.SchemaID,
			)
		}
		if channel.Topic != previous.Topic {
			doctor.error("Two channel records with same ID %d but different topics (%q != %q)",
				channel.ID,
				channel.Topic,
				previous.Topic,
			)
		}
		if channel.MessageEncoding != previous.MessageEncoding {
			doctor.error("Two channel records with same ID %d but different message encodings (%q != %q)",
				channel.ID,
				channel.MessageEncoding,
				previous.MessageEncoding,
			)
		}
		if !reflect.DeepEqual(channel.Metadata, previous.Metadata) {
			doctor.error("Two channel records with different metadata present with same ID %d",
				channel.ID)
		}
	}
	if doctor.inSummarySection {
		if previous == nil {
			doctor.error("Channel with ID %d in summary section does not exist in data section", channel.ID)
		}
		doctor.channelIDsInSummarySection[channel.ID] = true
	} else {
		if previous != nil {
			doctor.warn("Duplicate channel records in data section with ID %d", channel.ID)
		}
		doctor.channelsInDataSection[channel.ID] = channel
	}

	if channel.SchemaID != 0 {
		if _, ok := doctor.schemasInDataSection[channel.SchemaID]; !ok {
			doctor.error("Encountered Channel (%d) with unknown Schema (%d)", channel.ID, channel.SchemaID)
		}
	}
}

func (doctor *mcapDoctor) examineChunk(chunk *mcap.Chunk, startOffset uint64) {
	referencedChannels := make(map[uint16]bool)
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
		doctor.error("Unsupported compression format: %q", chunk.Compression)
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
		SkipMagic:         true,
		ValidateChunkCRCs: true,
		EmitChunks:        true,
	})
	if err != nil {
		doctor.error("Failed to make lexer for chunk bytes: %s", err)
		return
	}
	defer lexer.Close()

	var minLogTime uint64 = math.MaxUint64
	var maxLogTime uint64
	var chunkMessageCount uint64

	msg := make([]byte, 1024)
	for {
		tokenType, data, err := lexer.Next(msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			doctor.fatalf("Failed to read next token: %s", err)
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
				doctor.error("Failed to parse schema: %s", err)
			}
			doctor.examineSchema(schema)
		case mcap.TokenChannel:
			channel, err := mcap.ParseChannel(data)
			if err != nil {
				doctor.error("Error parsing Channel: %s", err)
			}
			doctor.examineChannel(channel)
		case mcap.TokenMessage:
			message, err := mcap.ParseMessage(data)
			if err != nil {
				doctor.error("Error parsing Message: %s", err)
			}
			referencedChannels[message.ChannelID] = true

			channel := doctor.channelsInDataSection[message.ChannelID]
			if channel == nil {
				doctor.error("Got a Message record for channel: %d before a channel record.", message.ChannelID)
			}

			if message.LogTime < doctor.maxLogTime {
				errStr := fmt.Sprintf("Message.log_time %d on %q is less than the latest log time %d",
					message.LogTime, channel.Topic, doctor.maxLogTime)
				if strictMessageOrder {
					doctor.error(errStr)
				} else {
					doctor.warn(errStr)
				}
			}

			if message.LogTime < minLogTime {
				minLogTime = message.LogTime
			}

			if message.LogTime > maxLogTime {
				maxLogTime = message.LogTime
			}

			if message.LogTime > doctor.maxLogTime {
				doctor.maxLogTime = message.LogTime
			}

			chunkMessageCount++
			doctor.messageCount++

		default:
			doctor.error("Illegal record in chunk: %d", tokenType)
		}
	}

	if chunkMessageCount != 0 {
		if minLogTime != chunk.MessageStartTime {
			doctor.error(
				"Chunk.message_start_time %d does not match the earliest message log time %d",
				chunk.MessageStartTime,
				minLogTime,
			)
		}

		if maxLogTime != chunk.MessageEndTime && chunkMessageCount != 0 {
			doctor.error(
				"Chunk.message_end_time %d does not match the latest message log time %d",
				chunk.MessageEndTime,
				maxLogTime,
			)
		}

		if minLogTime < doctor.minLogTime {
			doctor.minLogTime = minLogTime
		}
		if maxLogTime > doctor.maxLogTime {
			doctor.maxLogTime = maxLogTime
		}
	}
	asArray := make([]uint16, 0, len(referencedChannels))
	for id := range referencedChannels {
		asArray = append(asArray, id)
	}
	doctor.channelsReferencedInChunksByOffset[startOffset] = asArray
}

type Diagnosis struct {
	Errors   []string
	Warnings []string
}

func (doctor *mcapDoctor) Examine() Diagnosis {
	lexer, err := mcap.NewLexer(doctor.reader, &mcap.LexerOptions{
		SkipMagic:         false,
		ValidateChunkCRCs: true,
		EmitChunks:        true,
		AttachmentCallback: func(attachment *mcap.AttachmentReader) error {
			if !utf8.ValidString(attachment.Name) {
				doctor.error("Attachment name %q is not valid utf-8", attachment.Name)
			}
			if !utf8.ValidString(attachment.MediaType) {
				doctor.error("Attachment media type %q is not valid utf-8", attachment.MediaType)
			}
			return nil
		},
	})
	if err != nil {
		doctor.fatal(err)
	}
	defer lexer.Close()

	var lastMessageTime uint64
	var lastToken mcap.TokenType
	var dataEnd *mcap.DataEnd
	var footer *mcap.Footer
	var messageOutsideChunk bool
	msg := make([]byte, 1024)
	for {
		tokenType, data, err := lexer.Next(msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if dataEnd == nil {
					doctor.error("File does not contain a DataEnd record (last record was %s)", lastToken.String())
				}
				if footer == nil {
					doctor.error("File does not contain a Footer record (last record was %s)", lastToken.String())
				}
				break
			}
			doctor.fatalf("Failed to read next token: %s", err)
		}
		lastToken = tokenType
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
				doctor.warn("Set the Header.library field to a value that identifies the software that produced the file.")
			}
			if !utf8.ValidString(header.Library) {
				doctor.error("header library is not valid utf-8: %q", header.Library)
			}
			if !utf8.ValidString(header.Profile) {
				doctor.error("header profile is not valid utf-8: %q", header.Profile)
			}
			if header.Profile != "" && header.Profile != "ros1" && header.Profile != "ros2" {
				doctor.warn(`Header.profile field %q is not a well-known profile.`, header.Profile)
			}
		case mcap.TokenFooter:
			footer, err = mcap.ParseFooter(data)
			if err != nil {
				doctor.error("Failed to parse footer: %s", err)
			}
		case mcap.TokenSchema:
			schema, err := mcap.ParseSchema(data)
			if err != nil {
				doctor.error("Failed to parse schema: %s", err)
			}
			doctor.examineSchema(schema)
		case mcap.TokenChannel:
			channel, err := mcap.ParseChannel(data)
			if err != nil {
				doctor.error("Error parsing Channel: %s", err)
			}
			doctor.examineChannel(channel)
		case mcap.TokenMessage:
			message, err := mcap.ParseMessage(data)
			if err != nil {
				doctor.error("Error parsing Message: %s", err)
			}
			messageOutsideChunk = true
			channel := doctor.channelsInDataSection[message.ChannelID]
			if channel == nil {
				doctor.error("Got a Message record for channel: %d before a channel info.", message.ChannelID)
			}
			if message.LogTime < lastMessageTime {
				doctor.error("Message.log_time %d on %q is less than the previous message record time %d",
					message.LogTime, channel.Topic, lastMessageTime)
			}
			lastMessageTime = message.LogTime

			if message.LogTime < doctor.minLogTime {
				doctor.minLogTime = message.LogTime
			}
			if message.LogTime > doctor.maxLogTime {
				doctor.maxLogTime = message.LogTime
			}

			doctor.messageCount++

		case mcap.TokenChunk:
			chunk, err := mcap.ParseChunk(data)
			if err != nil {
				doctor.error("Error parsing Message: %s", err)
			}
			pos, err := doctor.reader.Seek(0, io.SeekCurrent)
			if err != nil {
				// cannot continue if seek fails
				doctor.fatalf("failed to determine read cursor: %s", err)
			}
			chunkStartOffset := uint64(pos - int64(len(data)) - 9)
			doctor.examineChunk(chunk, chunkStartOffset)
		case mcap.TokenMessageIndex:
			_, err := mcap.ParseMessageIndex(data)
			if err != nil {
				doctor.error("Failed to parse message index: %s", err)
			}
			if messageOutsideChunk {
				doctor.warn("Message index in file has message records outside chunks. Indexed readers will miss these messages.")
			}
		case mcap.TokenChunkIndex:
			chunkIndex, err := mcap.ParseChunkIndex(data)
			if err != nil {
				doctor.error("Failed to parse chunk index: %s", err)
			}
			if messageOutsideChunk {
				doctor.warn("Message index in file has message records outside chunks. Indexed readers will miss these messages.")
			}
			if _, ok := doctor.chunkIndexes[chunkIndex.ChunkStartOffset]; ok {
				doctor.error("Multiple chunk indexes found for chunk at offset %d", chunkIndex.ChunkStartOffset)
			}
			doctor.chunkIndexes[chunkIndex.ChunkStartOffset] = chunkIndex
		case mcap.TokenAttachmentIndex:
			index, err := mcap.ParseAttachmentIndex(data)
			if err != nil {
				doctor.error("Failed to parse attachment index: %s", err)
			}
			if !utf8.ValidString(index.Name) {
				doctor.error("Attachment name %q in index is not valid utf-8", index.Name)
			}
			if !utf8.ValidString(index.MediaType) {
				doctor.error("Attachment media type %q in index is not valid utf-8", index.MediaType)
			}
		case mcap.TokenStatistics:
			statistics, err := mcap.ParseStatistics(data)
			if err != nil {
				doctor.error("Failed to parse statistics: %s", err)
			}
			if doctor.statistics != nil {
				doctor.error("File contains multiple Statistics records")
			}
			doctor.statistics = statistics
		case mcap.TokenMetadata:
			metadataRecord, err := mcap.ParseMetadata(data)
			if err != nil {
				doctor.error("Failed to parse metadata: %s", err)
			}
			if !utf8.ValidString(metadataRecord.Name) {
				doctor.error("metadata record name is not valid utf-8: %q", metadataRecord.Name)
			}
			for key, value := range metadataRecord.Metadata {
				if !utf8.ValidString(key) {
					doctor.error("metadata with name %q key is not valid utf-8: %q", metadataRecord.Name, key)
				}
				if !utf8.ValidString(value) {
					doctor.error("metadata with name %q value is not valid utf-8: %q", metadataRecord.Name, value)
				}
			}
		case mcap.TokenMetadataIndex:
			index, err := mcap.ParseMetadataIndex(data)
			if err != nil {
				doctor.error("Failed to parse metadata index: %s", err)
			}
			if !utf8.ValidString(index.Name) {
				doctor.error("Metadata name %q in index is not valid utf-8", index.Name)
			}
		case mcap.TokenSummaryOffset:
			_, err := mcap.ParseSummaryOffset(data)
			if err != nil {
				doctor.error("Failed to parse summary offset: %s", err)
			}
		case mcap.TokenDataEnd:
			dataEnd, err = mcap.ParseDataEnd(data)
			if err != nil {
				doctor.error("Failed to parse data end: %s", err)
			}
			doctor.inSummarySection = true
		case mcap.TokenError:
			// this is the value of the tokenType when there is an error
			// from the lexer, which we caught at the top.
			doctor.fatalf("Failed to parse:", err)
		}
	}

	for chunkOffset, chunkIndex := range doctor.chunkIndexes {
		channelsReferenced := doctor.channelsReferencedInChunksByOffset[chunkOffset]
		for _, id := range channelsReferenced {
			if present := doctor.channelIDsInSummarySection[id]; !present {
				doctor.error(
					"Indexed chunk at offset %d contains messages referencing channel (%d) not duplicated in summary section",
					chunkOffset,
					id,
				)
			}
			channel := doctor.channelsInDataSection[id]
			if channel == nil {
				// message with unknown channel, this is checked when that message is scanned
				continue
			}
			if channel.SchemaID == 0 {
				continue
			}
			if present := doctor.schemaIDsInSummarySection[channel.SchemaID]; !present {
				doctor.error(
					"Indexed chunk at offset %d contains messages referencing schema (%d) not duplicated in summary section",
					chunkOffset,
					channel.SchemaID,
				)
			}
		}

		_, err := doctor.reader.Seek(int64(chunkOffset), io.SeekStart)
		if err != nil {
			doctor.fatalf("failed to seek to chunk offset: %s", err)
		}
		tokenType, data, err := lexer.Next(msg)
		if err != nil {
			doctor.error("Chunk index points to offset %d but encountered error reading at that offset: %v", chunkOffset, err)
			continue
		}
		if tokenType != mcap.TokenChunk {
			doctor.error(
				"Chunk index points to offset %d but the record at this offset is a %s",
				chunkOffset,
				tokenType.String(),
			)
			continue
		}
		if chunkIndex.ChunkLength != 9+uint64(len(data)) {
			doctor.error(
				"Chunk index %d length mismatch: %d vs %d.",
				chunkOffset,
				chunkIndex.ChunkLength,
				9+len(data),
			)
			continue
		}
		chunk, err := mcap.ParseChunk(data)
		if err != nil {
			doctor.error(
				"Chunk index points to offset %d but encountered error parsing the chunk at that offset: %v",
				chunkOffset,
				err,
			)
			continue
		}
		if chunk.MessageStartTime != chunkIndex.MessageStartTime {
			doctor.error(
				"Chunk at offset %d has message start time %d, but its chunk index has message start time %d",
				chunkOffset,
				chunk.MessageStartTime,
				chunkIndex.MessageStartTime,
			)
		}
		if chunk.MessageEndTime != chunkIndex.MessageEndTime {
			doctor.error(
				"Chunk at offset %d has message end time %d, but its chunk index has message end time %d",
				chunkOffset,
				chunk.MessageEndTime,
				chunkIndex.MessageEndTime,
			)
		}
		if chunk.Compression != chunkIndex.Compression.String() {
			doctor.error(
				"Chunk at offset %d has compression %q, but its chunk index has compression %q",
				chunkOffset,
				chunk.Compression,
				chunkIndex.Compression,
			)
		}
		if uint64(len(chunk.Records)) != chunkIndex.CompressedSize {
			doctor.error(
				"Chunk at offset %d has data length %d, but its chunk index has compressed size %d",
				chunkOffset,
				len(chunk.Records),
				chunkIndex.CompressedSize,
			)
		}
		if chunk.UncompressedSize != chunkIndex.UncompressedSize {
			doctor.error(
				"Chunk at offset %d has uncompressed size %d, but its chunk index has uncompressed size %d",
				chunkOffset,
				chunk.UncompressedSize,
				chunkIndex.UncompressedSize,
			)
		}
	}

	if doctor.statistics != nil {
		if doctor.messageCount > 0 {
			if doctor.statistics.MessageStartTime != doctor.minLogTime {
				doctor.error(
					"Statistics has message start time %d, but the minimum message start time is %d",
					doctor.statistics.MessageStartTime,
					doctor.minLogTime,
				)
			}
			if doctor.statistics.MessageEndTime != doctor.maxLogTime {
				doctor.error(
					"Statistics has message end time %d, but the maximum message end time is %d",
					doctor.statistics.MessageEndTime,
					doctor.maxLogTime,
				)
			}
		}
		if doctor.statistics.MessageCount != doctor.messageCount {
			doctor.error(
				"Statistics has message count %d, but actual number of messages is %d",
				doctor.statistics.MessageCount,
				doctor.messageCount,
			)
		}
	}
	return doctor.diagnosis
}

func newMcapDoctor(reader io.ReadSeeker) *mcapDoctor {
	return &mcapDoctor{
		reader:                             reader,
		channelsInDataSection:              make(map[uint16]*mcap.Channel),
		channelsReferencedInChunksByOffset: make(map[uint64][]uint16),
		channelIDsInSummarySection:         make(map[uint16]bool),
		schemaIDsInSummarySection:          make(map[uint16]bool),
		schemasInDataSection:               make(map[uint16]*mcap.Schema),
		chunkIndexes:                       make(map[uint64]*mcap.ChunkIndex),
		minLogTime:                         math.MaxUint64,
	}
}

func main(_ *cobra.Command, args []string) {
	ctx := context.Background()
	if len(args) != 1 {
		fmt.Println("An MCAP file argument is required.")
		os.Exit(1)
	}
	filename := args[0]
	err := utils.WithReader(ctx, filename, func(remote bool, rs io.ReadSeeker) error {
		doctor := newMcapDoctor(rs)
		if remote {
			color.Yellow("Will read full remote file")
		}
		if verbose {
			fmt.Printf("Examining %s\n", args[0])
		}
		diagnosis := doctor.Examine()
		if len(diagnosis.Errors) > 0 {
			return fmt.Errorf("encountered %d errors", len(diagnosis.Errors))
		}
		return nil
	})
	if err != nil {
		die("Doctor command failed: %s", err)
	}
}

var doctorCommand = &cobra.Command{
	Use:   "doctor <file>",
	Short: "Check an MCAP file structure",
	Run:   main,
}

func init() {
	rootCmd.AddCommand(doctorCommand)

	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output")
	rootCmd.PersistentFlags().BoolVarP(&strictMessageOrder, "strict-message-order", "",
		false, "Require that messages have a monotonic log time")
}
