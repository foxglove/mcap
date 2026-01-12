package mcap

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// Magic is the magic number for an MCAP file.
var Magic = []byte{0x89, 'M', 'C', 'A', 'P', 0x30, '\r', '\n'}

const (
	// CompressionZSTD represents zstd compression.
	CompressionZSTD CompressionFormat = "zstd"
	// CompressionLZ4 represents lz4 compression.
	CompressionLZ4 CompressionFormat = "lz4"
	// CompressionNone represents no compression.
	CompressionNone CompressionFormat = ""
)

// CompressionFormat represents a supported chunk compression format.
type CompressionFormat string

// String converts a compression format to a string for display.
func (c CompressionFormat) String() string {
	return string(c)
}

const (
	OpReserved        OpCode = 0x00
	OpHeader          OpCode = 0x01
	OpFooter          OpCode = 0x02
	OpSchema          OpCode = 0x03
	OpChannel         OpCode = 0x04
	OpMessage         OpCode = 0x05
	OpChunk           OpCode = 0x06
	OpMessageIndex    OpCode = 0x07
	OpChunkIndex      OpCode = 0x08
	OpAttachment      OpCode = 0x09
	OpAttachmentIndex OpCode = 0x0A
	OpStatistics      OpCode = 0x0B
	OpMetadata        OpCode = 0x0C
	OpMetadataIndex   OpCode = 0x0D
	OpSummaryOffset   OpCode = 0x0E
	OpDataEnd         OpCode = 0x0F
)

type OpCode byte

func (c OpCode) String() string {
	switch c {
	case OpReserved:
		return "reserved"
	case OpHeader:
		return "header"
	case OpFooter:
		return "footer"
	case OpSchema:
		return "schema"
	case OpChannel:
		return "channel"
	case OpMessage:
		return "message"
	case OpChunk:
		return "chunk"
	case OpMessageIndex:
		return "message index"
	case OpChunkIndex:
		return "chunk index"
	case OpAttachment:
		return "attachment"
	case OpAttachmentIndex:
		return "attachment index"
	case OpStatistics:
		return "statistics"
	case OpMetadata:
		return "metadata"
	case OpMetadataIndex:
		return "metadata index"
	case OpSummaryOffset:
		return "summary offset"
	case OpDataEnd:
		return "data end"
	default:
		return fmt.Sprintf("<unrecognized opcode 0x%02x>", byte(c))
	}
}

// Header is the first record in an MCAP file.
type Header struct {
	Profile string
	Library string
}

// Footer records contain end-of-file information. MCAP files must end with a
// Footer record.
type Footer struct {
	SummaryStart       uint64
	SummaryOffsetStart uint64
	SummaryCRC         uint32
}

// A Schema record defines an individual schema. Schema records are uniquely
// identified within a file by their schema ID. A Schema record must occur at
// least once in the file prior to any Channel referring to its ID. Any two
// schema records sharing a common ID must be identical.
type Schema struct {
	ID       uint16
	Name     string
	Encoding string
	Data     []byte
}

// Channel records define encoded streams of messages on topics. Channel records
// are uniquely identified within a file by their channel ID. A Channel record
// must occur at least once in the file prior to any message referring to its
// channel ID. Any two channel records sharing a common ID must be identical.
type Channel struct {
	ID              uint16
	SchemaID        uint16
	Topic           string
	MessageEncoding string
	Metadata        map[string]string
}

// Message records encode a single timestamped message on a channel. The message
// encoding and schema must match that of the Channel record corresponding to
// the message's channel ID.
type Message struct {
	ChannelID   uint16
	Sequence    uint32
	LogTime     uint64
	PublishTime uint64
	Data        []byte
}

// Chunk records each contain a batch of Schema, Channel, and Message records.
// The batch of records contained in a chunk may be compressed or uncompressed.
// All messages in the chunk must reference channels recorded earlier in the
// file (in a previous chunk or earlier in the current chunk).
type Chunk struct {
	MessageStartTime uint64
	MessageEndTime   uint64
	UncompressedSize uint64
	UncompressedCRC  uint32
	Compression      string
	Records          []byte
}

// MessageIndex records allow readers to locate individual records within a
// chunk by timestamp. A sequence of Message Index records occurs immediately
// after each chunk. Exactly one Message Index record must exist in the sequence
// for every channel on which a message occurs inside the chunk.
type MessageIndex struct {
	ChannelID    uint16
	Records      []MessageIndexEntry
	currentIndex int
}

// Reset resets the MessageIndex to an empty state, to enable reuse.
func (idx *MessageIndex) Reset() {
	idx.currentIndex = 0
}

func (idx *MessageIndex) IsEmpty() bool {
	return idx.currentIndex == 0
}

// Entries lists the entries in the message index.
func (idx *MessageIndex) Entries() []MessageIndexEntry {
	return idx.Records[:idx.currentIndex]
}

// Add an entry to the message index.
func (idx *MessageIndex) Add(timestamp uint64, offset uint64) {
	if idx.currentIndex >= len(idx.Records) {
		records := make([]MessageIndexEntry, (len(idx.Records)+20)*2)
		copy(records, idx.Records)
		idx.Records = records
	}
	idx.Records[idx.currentIndex].Timestamp = timestamp
	idx.Records[idx.currentIndex].Offset = offset
	idx.currentIndex++
}

// ChunkIndex records contain the location of a Chunk record and its associated
// MessageIndex records. A ChunkIndex record exists for every Chunk in the file.
type ChunkIndex struct {
	MessageStartTime    uint64
	MessageEndTime      uint64
	ChunkStartOffset    uint64
	ChunkLength         uint64
	MessageIndexOffsets map[uint16]uint64
	MessageIndexLength  uint64
	Compression         CompressionFormat
	CompressedSize      uint64
	UncompressedSize    uint64
}

// Attachment records contain auxiliary artifacts such as text, core dumps,
// calibration data, or other arbitrary data. Attachment records must not appear
// within a chunk.
type Attachment struct {
	LogTime    uint64
	CreateTime uint64
	Name       string
	MediaType  string
	DataSize   uint64
	Data       io.Reader
}

// AttachmentReader represents an attachment for handling in a streaming manner.
type AttachmentReader struct {
	LogTime    uint64
	CreateTime uint64
	Name       string
	MediaType  string
	DataSize   uint64

	data       *io.LimitedReader
	baseReader io.Reader
	crcReader  *crcReader
	crc        *uint32
}

// ComputedCRC discards any remaining data in the Data portion of the
// AttachmentReader, then returns the checksum computed from the fields of the
// attachment up to the CRC. If it is called before the data portion of the
// reader has been fully consumed, an error will be returned. If the
// AttachmentReader has been created with a crcReader that is instructed not to
// compute the CRC, this will return a CRC of zero.
func (ar *AttachmentReader) ComputedCRC() (uint32, error) {
	if ar.data.N > 0 {
		return 0, fmt.Errorf("attachment CRC requested with unhandled data")
	}
	return ar.crcReader.Checksum(), nil
}

// ParsedCRC returns the CRC from the crc field of the record. It must be called
// after the data field has been handled. If ParsedCRC is called before the data
// reader is exhausted, an error is returned.
func (ar *AttachmentReader) ParsedCRC() (uint32, error) {
	if ar.crc != nil {
		return *ar.crc, nil
	}
	if ar.data.N > 0 {
		return 0, fmt.Errorf("attachment CRC requested with unhandled data")
	}
	buf := make([]byte, 4)
	_, err := io.ReadFull(ar.baseReader, buf)
	if err != nil {
		return 0, fmt.Errorf("failed to read CRC: %w", err)
	}
	crc := binary.LittleEndian.Uint32(buf)
	ar.crc = &crc
	return crc, nil
}

// Data returns a reader over the data section of the attachment.
func (ar *AttachmentReader) Data() io.Reader {
	return ar.data
}

// AttachmentIndex records contain the location of attachments in the file. An
// AttachmentIndex record exists for every Attachment in the file.
type AttachmentIndex struct {
	Offset     uint64
	Length     uint64
	LogTime    uint64
	CreateTime uint64
	DataSize   uint64
	Name       string
	MediaType  string
}

// Statistics records contain summary information about recorded data. The
// statistics record is optional, but the file should contain at most one.
type Statistics struct {
	MessageCount         uint64
	SchemaCount          uint16
	ChannelCount         uint32
	AttachmentCount      uint32
	MetadataCount        uint32
	ChunkCount           uint32
	MessageStartTime     uint64
	MessageEndTime       uint64
	ChannelMessageCounts map[uint16]uint64
}

// Metadata records contain arbitrary user data in key-value pairs.
type Metadata struct {
	Name     string
	Metadata map[string]string
}

// MetadataIndex records each contain the location of a metadata record within the file.
type MetadataIndex struct {
	Offset uint64
	Length uint64
	Name   string
}

// SummaryOffset records contain the location of records within the summary
// section. Each SummaryOffset record corresponds to a group of summary records
// with a common opcode.
type SummaryOffset struct {
	GroupOpcode OpCode
	GroupStart  uint64
	GroupLength uint64
}

// DataEnd indicates the end of the data section.
type DataEnd struct {
	DataSectionCRC uint32
}

// Info represents the result of an "info" operation, for gathering information
// from the summary section of a file.
type Info struct {
	Statistics        *Statistics
	Channels          map[uint16]*Channel
	Schemas           map[uint16]*Schema
	ChunkIndexes      []*ChunkIndex
	MetadataIndexes   []*MetadataIndex
	AttachmentIndexes []*AttachmentIndex
	Header            *Header
	Footer            *Footer
}

// ChannelCounts counts the number of messages on each channel in an Info.
func (i *Info) ChannelCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	for k, v := range i.Statistics.ChannelMessageCounts {
		channel := i.Channels[k]
		counts[channel.Topic] = v
	}
	return counts
}

// CanReadMessagesUsingIndex returns true if messages can be read from this file efficiently using
// the index.
func (i *Info) CanReadMessagesUsingIndex() bool {
	// If there are chunk indexes, we can read messages using the index.
	// if there are none, but the statistics indicate that there are messages, then we know
	// that a read using the indexed message iterator will still yield the correct set of messages.
	return len(i.ChunkIndexes) > 0 || (i.Statistics != nil && i.Statistics.MessageCount == 0)
}

type MessageIndexEntry struct {
	Timestamp uint64
	Offset    uint64
}

func makeSafe(n uint64) ([]byte, error) {
	if n < math.MaxInt32 {
		return make([]byte, n), nil
	}
	return nil, ErrLengthOutOfRange
}
