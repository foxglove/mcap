package libmcap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"sort"
	"time"
)

var (
	Magic = []byte{0x89, 'M', 'C', 'A', 'P', 0x30, '\r', '\n'}
)

const (
	CompressionLZ4  CompressionFormat = "lz4"
	CompressionZSTD CompressionFormat = "zstd"
	CompressionNone CompressionFormat = "none"
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

func putByte(buf []byte, x byte) int {
	buf[0] = x
	return 1
}

func getUint16(buf []byte, offset int) (uint16, int) {
	return binary.LittleEndian.Uint16(buf[offset:]), offset + 2
}

func getUint32(buf []byte, offset int) (uint32, int) {
	return binary.LittleEndian.Uint32(buf[offset:]), offset + 4
}

func getUint64(buf []byte, offset int) (uint64, int) {
	return binary.LittleEndian.Uint64(buf[offset:]), offset + 8
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

const (
	OpHeader          OpCode = 0x01
	OpFooter          OpCode = 0x02
	OpChannelInfo     OpCode = 0x03
	OpMessage         OpCode = 0x04
	OpChunk           OpCode = 0x05
	OpMessageIndex    OpCode = 0x06
	OpChunkIndex      OpCode = 0x07
	OpAttachment      OpCode = 0x08
	OpAttachmentIndex OpCode = 0x09
	OpStatistics      OpCode = 0x0a
)

type OpCode byte

type Message struct {
	ChannelID   uint16
	Sequence    uint32
	RecordTime  uint64
	PublishTime uint64
	Data        []byte
	channelInfo *ChannelInfo
}

type ChannelInfo struct {
	ChannelID  uint16
	TopicName  string
	Encoding   string
	SchemaName string
	Schema     []byte
	UserData   map[string]string
}

type Attachment struct {
	Name        string
	RecordTime  uint64
	ContentType string
	Data        []byte
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

// TODO md5sum in rosbags does not have a place in mcap

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
	RecordTime     uint64
	AttachmentSize uint64
	Name           string
	ContentType    string
	Offset         uint64
}

type Footer struct {
	IndexOffset uint64
	IndexCRC    uint32
}

type ChunkIndex struct {
	StartTime           uint64
	EndTime             uint64
	ChunkOffset         uint64
	MessageIndexOffsets map[uint16]uint64
	MessageIndexLength  uint64
	Compression         CompressionFormat
	CompressedSize      uint64
	UncompressedSize    uint64
}

type Statistics struct {
	MessageCount    uint64
	ChannelCount    uint32
	AttachmentCount uint32
	ChunkCount      uint32
	ChannelStats    map[uint16]uint64
	channels        map[uint16]*ChannelInfo
}

type Info struct {
	Statistics   *Statistics
	Channels     map[uint16]*ChannelInfo
	ChunkIndexes []*ChunkIndex
	Start        time.Time
	End          time.Time
}

func (i Info) ChannelCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	for k, v := range i.Statistics.ChannelStats {
		channel := i.Channels[k]
		counts[channel.TopicName] = v
	}
	return counts
}

func (i Info) String() string {
	buf := &bytes.Buffer{}
	start := uint64(math.MaxUint64)
	end := uint64(0)

	compressionFormatStats := make(map[CompressionFormat]struct {
		count            int
		compressedSize   uint64
		uncompressedSize uint64
	})
	for _, ci := range i.ChunkIndexes {
		if ci.StartTime < start {
			start = ci.StartTime
		}
		if ci.EndTime > end {
			end = ci.EndTime
		}
		stats := compressionFormatStats[ci.Compression]
		stats.count++
		stats.compressedSize += ci.CompressedSize
		stats.uncompressedSize += ci.UncompressedSize
		compressionFormatStats[ci.Compression] = stats
	}

	starttime := time.Unix(int64(start/1e9), int64(start%1e9))
	endtime := time.Unix(int64(end/1e9), int64(end%1e9))

	fmt.Fprintf(buf, "duration: %s\n", endtime.Sub(starttime))
	fmt.Fprintf(buf, "start: %s\n", starttime.Format(time.RFC3339Nano))
	fmt.Fprintf(buf, "end: %s\n", endtime.Format(time.RFC3339Nano))
	fmt.Fprintf(buf, "messages: %d\n", i.Statistics.MessageCount)
	fmt.Fprintf(buf, "chunks:\n")
	chunkCount := len(i.ChunkIndexes)
	for k, v := range compressionFormatStats {
		compressionRatio := 100 * (1 - float64(v.compressedSize)/float64(v.uncompressedSize))
		fmt.Fprintf(buf, "\t%s: [%d/%d chunks] (%.2f%%) \n", k, v.count, chunkCount, compressionRatio)
	}
	fmt.Fprintf(buf, "channels:\n")

	chanIDs := []uint16{}
	for chanID := range i.Channels {
		chanIDs = append(chanIDs, chanID)
	}
	sort.Slice(chanIDs, func(i, j int) bool {
		return chanIDs[i] < chanIDs[j]
	})
	for _, chanID := range chanIDs {
		channel := i.Channels[chanID]
		fmt.Fprintf(buf, "\t(%d) %s: %d msgs\n", channel.ChannelID, channel.TopicName, i.Statistics.ChannelStats[chanID])
	}
	fmt.Fprintf(buf, "attachments: %d", i.Statistics.AttachmentCount)
	return buf.String()
}

type MessageIndexRecord struct {
	Timestamp uint64
	Offset    uint64
}

type MessageIndex struct {
	ChannelID uint16
	Count     uint32
	Records   []MessageIndexRecord
	CRC       uint32
}

type Chunk struct {
	UncompressedSize uint64
	UncompressedCRC  uint32
	Compression      string
	Records          []byte
}
