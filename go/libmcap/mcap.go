package libmcap

import (
	"bytes"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"time"
)

var (
	Magic = []byte{0x89, 'M', 'C', 'A', 'P', 0x30, '\r', '\n'}
)

const (
	CompressionZSTD CompressionFormat = "zstd"
	CompressionLZ4  CompressionFormat = "lz4"
	CompressionNone CompressionFormat = ""
)

type BufCloser struct {
	b *bytes.Buffer
}

func (b BufCloser) Close() error {
	return nil
}

func (b BufCloser) Write(p []byte) (int, error) {
	return b.b.Write(p)
}

func (b BufCloser) Reset(w io.Writer) {
	b.b.Reset()
}

type ResettableReader interface {
	io.ReadCloser
	Reset(io.Reader)
}

type ResettableWriteCloser interface {
	io.WriteCloser
	Reset(io.Writer)
}

type CRCWriter struct {
	w   io.Writer
	crc hash.Hash32
}

func (w *CRCWriter) Write(p []byte) (int, error) {
	_, _ = w.crc.Write(p)
	return w.w.Write(p)
}

func (w *CRCWriter) Checksum() uint32 {
	return w.crc.Sum32()
}

func (w *CRCWriter) Reset() {
	w.crc = crc32.NewIEEE()
}

func NewCRCWriter(w io.Writer) *CRCWriter {
	return &CRCWriter{
		w:   w,
		crc: crc32.NewIEEE(),
	}
}

type WriteSizer struct {
	w    *CRCWriter
	size uint64
}

func (w *WriteSizer) Write(p []byte) (int, error) {
	w.size += uint64(len(p))
	return w.w.Write(p)
}

func NewWriteSizer(w io.Writer) *WriteSizer {
	return &WriteSizer{
		w: NewCRCWriter(w),
	}
}

func (w *WriteSizer) Size() uint64 {
	return w.size
}

func (w *WriteSizer) Checksum() uint32 {
	return w.w.Checksum()
}

func (w *WriteSizer) Reset() {
	w.w.crc = crc32.NewIEEE()
}

func putByte(buf []byte, x byte) (int, error) {
	if len(buf) < 1 {
		return 0, io.ErrShortBuffer
	}
	buf[0] = x
	return 1, nil
}

func getUint16(buf []byte, offset int) (x uint16, newoffset int, err error) {
	if offset > len(buf)-2 {
		return 0, 0, io.ErrShortBuffer
	}
	return binary.LittleEndian.Uint16(buf[offset:]), offset + 2, nil
}

func getUint32(buf []byte, offset int) (x uint32, newoffset int, err error) {
	if offset > len(buf)-4 {
		return 0, 0, io.ErrShortBuffer
	}
	return binary.LittleEndian.Uint32(buf[offset:]), offset + 4, nil
}

func getUint64(buf []byte, offset int) (x uint64, newoffset int, err error) {
	if offset > len(buf)-8 {
		return 0, 0, io.ErrShortBuffer
	}
	return binary.LittleEndian.Uint64(buf[offset:]), offset + 8, nil
}

func putUint16(buf []byte, i uint16) int {
	binary.LittleEndian.PutUint16(buf, i)
	return 2
}

func putUint32(buf []byte, i uint32) int {
	binary.LittleEndian.PutUint32(buf, i)
	return 4
}

func putUint64(buf []byte, i uint64) int {
	binary.LittleEndian.PutUint64(buf, i)
	return 8
}

func putPrefixedString(buf []byte, s string) int {
	offset := putUint32(buf, uint32(len(s)))
	offset += copy(buf[offset:], s)
	return offset
}

func putPrefixedBytes(buf []byte, s []byte) int {
	offset := putUint32(buf, uint32(len(s)))
	offset += copy(buf[offset:], s)
	return offset
}

type CompressionFormat string

func (c CompressionFormat) String() string {
	return string(c)
}

const (
	OpInvalidZero     OpCode = 0x00
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

type Header struct {
	Profile string
	Library string
}

type Message struct {
	ChannelID   uint16
	Sequence    uint32
	LogTime     uint64
	PublishTime uint64
	Data        []byte
}

type Channel struct {
	ID              uint16
	SchemaID        uint16
	Topic           string
	MessageEncoding string
	Metadata        map[string]string
}

type Attachment struct {
	Name        string
	CreatedAt   uint64
	LogTime     uint64
	ContentType string
	Data        []byte
	CRC         uint32
}

type CompressionSummary struct {
	Algorithm  CompressionFormat
	ChunkCount uint64
}

type TypeSummary struct {
	SchemaName string
}

type TopicSummary struct {
	TopicName    string
	MessageCount uint64
	SchemaName   string
}
type Summary struct {
	Duration    time.Duration
	Start       uint64
	End         uint64
	Size        uint64
	Messages    uint64
	Compression []CompressionSummary
	Types       []TypeSummary
	Topics      []TopicSummary
}

type AttachmentIndex struct {
	Offset      uint64
	Length      uint64
	LogTime     uint64
	DataSize    uint64
	Name        string
	ContentType string
}

type Schema struct {
	ID       uint16
	Name     string
	Encoding string
	Data     []byte
}

type Footer struct {
	SummaryStart       uint64
	SummaryOffsetStart uint64
	SummaryCRC         uint32
}

type SummaryOffset struct {
	GroupOpcode OpCode
	GroupStart  uint64
	GroupLength uint64
}

type Metadata struct {
	Name     string
	Metadata map[string]string
}

type MetadataIndex struct {
	Offset uint64
	Length uint64
	Name   string
}

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

type Statistics struct {
	MessageCount         uint64
	ChannelCount         uint32
	SchemaCount          uint32
	AttachmentCount      uint32
	MetadataCount        uint32
	ChunkCount           uint32
	MessageStartTime     uint64
	MessageEndTime       uint64
	ChannelMessageCounts map[uint16]uint64
}

type Info struct {
	Statistics   *Statistics
	Channels     map[uint16]*Channel
	Schemas      map[uint16]*Schema
	ChunkIndexes []*ChunkIndex
}

func (i *Info) ChannelCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	for k, v := range i.Statistics.ChannelMessageCounts {
		channel := i.Channels[k]
		counts[channel.Topic] = v
	}
	return counts
}

type MessageIndexEntry struct {
	Timestamp uint64
	Offset    uint64
}

type MessageIndex struct {
	ChannelID uint16
	Records   []MessageIndexEntry
}

type Chunk struct {
	MessageStartTime uint64
	MessageEndTime   uint64
	UncompressedSize uint64
	UncompressedCRC  uint32
	Compression      string
	Records          []byte
}

type DataEnd struct {
	DataSectionCRC uint32
}
