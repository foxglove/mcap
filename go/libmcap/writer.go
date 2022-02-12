package libmcap

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sort"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// ErrUnknownSchema is returned when a schema ID is not known to the writer.
var ErrUnknownSchema = errors.New("unknown schema")

// Writer is a writer for the MCAP format.
type Writer struct {
	// Statistics collected over the course of the recording.
	Statistics *Statistics
	// ChunkIndexes created over the course of the recording.
	ChunkIndexes []*ChunkIndex
	// AttachmentIndexes created over the course of the recording.
	AttachmentIndexes []*AttachmentIndex

	channelIDs       []uint16
	schemaIDs        []uint16
	channels         map[uint16]*Channel
	schemas          map[uint16]*Schema
	messageIndexes   map[uint16]*MessageIndex
	w                *WriteSizer
	buf              []byte
	msg              []byte
	chunk            []byte
	chunked          bool
	includeCRC       bool
	uncompressed     *bytes.Buffer
	chunksize        int64
	compressed       *bytes.Buffer
	compression      CompressionFormat
	compressedWriter *CountingCRCWriter

	currentChunkStartTime uint64
	currentChunkEndTime   uint64
}

// WriteHeader writes a header record to the output.
func (w *Writer) WriteHeader(header *Header) error {
	msglen := 4 + len(header.Profile) + 4 + len(header.Library)
	w.ensureSized(msglen)
	offset := putPrefixedString(w.msg, header.Profile)
	offset += putPrefixedString(w.msg[offset:], header.Library)
	_, err := w.writeRecord(w.w, OpHeader, w.msg[:offset])
	return err
}

// WriteFooter writes a footer record to the output. A Footer record contains
// end-of-file information. It must be the last record in the file. Readers
// using the index to read the file will begin with by reading the footer and
// trailing magic.
func (w *Writer) WriteFooter(f *Footer) error {
	msglen := 8 + 8 + 4
	w.ensureSized(msglen)
	offset := putUint64(w.msg, f.SummaryStart)
	offset += putUint64(w.msg[offset:], f.SummaryOffsetStart)
	offset += putUint32(w.msg[offset:], f.SummaryCRC)
	_, err := w.writeRecord(w.w, OpFooter, w.msg[:offset])
	return err
}

// WriteSchema writes a schema record to the output. Schema records are uniquely
// identified within a file by their schema ID. A Schema record must occur at
// least once in the file prior to any Channel Info referring to its ID.
func (w *Writer) WriteSchema(s *Schema) (err error) {
	msglen := 2 + 4 + len(s.Name) + 4 + len(s.Encoding) + 4 + len(s.Data)
	w.ensureSized(msglen)
	offset := putUint16(w.msg, s.ID)
	offset += putPrefixedString(w.msg[offset:], s.Name)
	offset += putPrefixedString(w.msg[offset:], s.Encoding)
	offset += putPrefixedBytes(w.msg[offset:], s.Data)
	if w.chunked {
		_, err = w.writeRecord(w.compressedWriter, OpSchema, w.msg[:offset])
	} else {
		_, err = w.writeRecord(w.w, OpSchema, w.msg[:offset])
	}
	if err != nil {
		return err
	}
	if _, ok := w.schemas[s.ID]; !ok {
		w.schemaIDs = append(w.schemaIDs, s.ID)
		w.schemas[s.ID] = s
		w.Statistics.SchemaCount++
	}
	return nil
}

// WriteChannel writes a channel info record to the output. Channel Info
// records are uniquely identified within a file by their channel ID. A Channel
// Info record must occur at least once in the file prior to any message
// referring to its channel ID.
func (w *Writer) WriteChannel(c *Channel) error {
	if c.SchemaID > 0 {
		if _, ok := w.schemas[c.SchemaID]; !ok {
			return ErrUnknownSchema
		}
	}
	userdata := makePrefixedMap(c.Metadata)
	msglen := (2 +
		4 + len(c.Topic) +
		4 + len(c.MessageEncoding) +
		2 +
		len(userdata))
	w.ensureSized(msglen)
	offset := putUint16(w.msg, c.ID)
	offset += putUint16(w.msg[offset:], c.SchemaID)
	offset += putPrefixedString(w.msg[offset:], c.Topic)
	offset += putPrefixedString(w.msg[offset:], c.MessageEncoding)
	offset += copy(w.msg[offset:], userdata)
	var err error
	if w.chunked {
		_, err = w.writeRecord(w.compressedWriter, OpChannel, w.msg[:offset])
		if err != nil {
			return err
		}
	} else {
		_, err = w.writeRecord(w.w, OpChannel, w.msg[:offset])
		if err != nil {
			return err
		}
	}
	if _, ok := w.channels[c.ID]; !ok {
		w.Statistics.ChannelCount++
		w.channels[c.ID] = c
		w.channelIDs = append(w.channelIDs, c.ID)
	}
	return nil
}

// WriteMessage writes a message to the output. A message record encodes a
// single timestamped message on a channel. The message encoding and schema must
// match that of the channel info record corresponding to the message's channel
// ID.
func (w *Writer) WriteMessage(m *Message) error {
	if w.channels[m.ChannelID] == nil {
		return fmt.Errorf("unrecognized channel %d", m.ChannelID)
	}
	msglen := 2 + 4 + 8 + 8 + len(m.Data)
	w.ensureSized(msglen)
	offset := putUint16(w.msg, m.ChannelID)
	offset += putUint32(w.msg[offset:], m.Sequence)
	offset += putUint64(w.msg[offset:], m.LogTime)
	offset += putUint64(w.msg[offset:], m.PublishTime)
	offset += copy(w.msg[offset:], m.Data)
	w.Statistics.ChannelMessageCounts[m.ChannelID]++
	w.Statistics.MessageCount++
	if w.chunked {
		// TODO preallocate or maybe fancy structure. These could be conserved
		// across chunks too, which might work ok assuming similar numbers of
		// messages/chan/chunk.
		idx, ok := w.messageIndexes[m.ChannelID]
		if !ok {
			idx = &MessageIndex{
				ChannelID: m.ChannelID,
				Records:   nil,
			}
			w.messageIndexes[m.ChannelID] = idx
		}
		idx.Add(m.LogTime, uint64(w.compressedWriter.Size()))
		_, err := w.writeRecord(w.compressedWriter, OpMessage, w.msg[:offset])
		if err != nil {
			return err
		}
		if w.compressedWriter.Size() > w.chunksize {
			err := w.flushActiveChunk()
			if err != nil {
				return err
			}
		}
		if m.LogTime > w.currentChunkEndTime {
			w.currentChunkEndTime = m.LogTime
		}
		if m.LogTime < w.currentChunkStartTime {
			w.currentChunkStartTime = m.LogTime
		}
	} else {
		_, err := w.writeRecord(w.w, OpMessage, w.msg[:offset])
		if err != nil {
			return err
		}
	}
	if m.LogTime > w.Statistics.MessageEndTime {
		w.Statistics.MessageEndTime = m.LogTime
	}
	if m.LogTime < w.Statistics.MessageStartTime {
		w.Statistics.MessageStartTime = m.LogTime
	}
	return nil
}

// WriteMessageIndex writes a message index record to the output. A Message
// Index record allows readers to locate individual message records within a
// chunk by their timestamp. A sequence of Message Index records occurs
// immediately after each chunk. Exactly one Message Index record must exist in
// the sequence for every channel on which a message occurs inside the chunk.
func (w *Writer) WriteMessageIndex(idx *MessageIndex) error {
	datalen := len(idx.Entries()) * (8 + 8)
	msglen := 2 + 4 + datalen
	w.ensureSized(msglen)
	offset := putUint16(w.msg, idx.ChannelID)
	offset += putUint32(w.msg[offset:], uint32(datalen))
	for _, v := range idx.Entries() {
		offset += putUint64(w.msg[offset:], v.Timestamp)
		offset += putUint64(w.msg[offset:], v.Offset)
	}
	_, err := w.writeRecord(w.w, OpMessageIndex, w.msg[:offset])
	return err
}

// WriteAttachment writes an attachment to the output. Attachment records
// contain auxiliary artifacts such as text, core dumps, calibration data, or
// other arbitrary data. Attachment records must not appear within a chunk.
func (w *Writer) WriteAttachment(a *Attachment) error {
	msglen := 4 + len(a.Name) + 8 + 8 + 4 + len(a.ContentType) + 8 + len(a.Data) + 4
	w.ensureSized(msglen)
	offset := putUint64(w.msg, a.LogTime)
	offset += putUint64(w.msg[offset:], a.CreateTime)
	offset += putPrefixedString(w.msg[offset:], a.Name)
	offset += putPrefixedString(w.msg[offset:], a.ContentType)
	offset += putUint64(w.msg[offset:], uint64(len(a.Data)))
	offset += copy(w.msg[offset:], a.Data)
	crc := crc32.ChecksumIEEE(w.msg[:offset])
	offset += putUint32(w.msg[offset:], crc)
	attachmentOffset := w.w.Size()
	c, err := w.writeRecord(w.w, OpAttachment, w.msg[:offset])
	if err != nil {
		return err
	}
	w.AttachmentIndexes = append(w.AttachmentIndexes, &AttachmentIndex{
		Offset:      attachmentOffset,
		Length:      uint64(c),
		LogTime:     a.LogTime,
		CreateTime:  a.CreateTime,
		DataSize:    uint64(len(a.Data)),
		Name:        a.Name,
		ContentType: a.ContentType,
	})
	w.Statistics.AttachmentCount++
	return nil
}

// WriteAttachmentIndex writes an attachment index record to the output. An
// Attachment Index record contains the location of an attachment in the file.
// An Attachment Index record exists for every Attachment record in the file.
func (w *Writer) WriteAttachmentIndex(idx *AttachmentIndex) error {
	msglen := 8 + 8 + 8 + 8 + 4 + len(idx.Name) + 4 + len(idx.ContentType)
	w.ensureSized(msglen)
	offset := putUint64(w.msg, idx.Offset)
	offset += putUint64(w.msg[offset:], idx.Length)
	offset += putUint64(w.msg[offset:], idx.LogTime)
	offset += putUint64(w.msg[offset:], idx.CreateTime)
	offset += putUint64(w.msg[offset:], idx.DataSize)
	offset += putPrefixedString(w.msg[offset:], idx.Name)
	offset += putPrefixedString(w.msg[offset:], idx.ContentType)
	_, err := w.writeRecord(w.w, OpAttachmentIndex, w.msg[:offset])
	return err
}

// WriteStatistics writes a statistics record to the output. A Statistics record
// contains summary information about the recorded data. The statistics record
// is optional, but the file should contain at most one.
func (w *Writer) WriteStatistics(s *Statistics) error {
	msglen := 8 + 2 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + len(s.ChannelMessageCounts)*(2+8)
	w.ensureSized(msglen)
	offset := putUint64(w.msg, s.MessageCount)
	offset += putUint16(w.msg[offset:], s.SchemaCount)
	offset += putUint32(w.msg[offset:], s.ChannelCount)
	offset += putUint32(w.msg[offset:], s.AttachmentCount)
	offset += putUint32(w.msg[offset:], s.MetadataCount)
	offset += putUint32(w.msg[offset:], s.ChunkCount)
	offset += putUint64(w.msg[offset:], s.MessageStartTime)
	offset += putUint64(w.msg[offset:], s.MessageEndTime)
	offset += putUint32(w.msg[offset:], uint32(len(s.ChannelMessageCounts)*(2+8)))
	for _, chanID := range w.channelIDs {
		if messageCount, ok := s.ChannelMessageCounts[chanID]; ok {
			offset += putUint16(w.msg[offset:], chanID)
			offset += putUint64(w.msg[offset:], messageCount)
		}
	}
	_, err := w.writeRecord(w.w, OpStatistics, w.msg[:offset])
	return err
}

// WriteMetadata writes a metadata record to the output. A metadata record
// contains arbitrary user data in key-value pairs.
func (w *Writer) WriteMetadata(m *Metadata) error {
	data := makePrefixedMap(m.Metadata)
	msglen := 4 + len(m.Name) + 4 + len(data)
	w.ensureSized(msglen)
	offset := putPrefixedString(w.msg, m.Name)
	offset += copy(w.msg[offset:], data)
	_, err := w.writeRecord(w.w, OpMetadata, w.msg[:offset])
	return err
}

// WriteMetadataIndex writes a metadata index record to the output.
func (w *Writer) WriteMetadataIndex(idx *MetadataIndex) error {
	msglen := 8 + 8 + 4 + len(idx.Name)
	w.ensureSized(msglen)
	offset := putUint64(w.msg, idx.Offset)
	offset += putUint64(w.msg[offset:], idx.Length)
	offset += putPrefixedString(w.msg[offset:], idx.Name)
	_, err := w.writeRecord(w.w, OpMetadataIndex, w.msg[:offset])
	return err
}

// WriteSummaryOffset writes a summary offset record to the output. A Summary
// Offset record contains the location of records within the summary section.
// Each Summary Offset record corresponds to a group of summary records with the
// same opcode.
func (w *Writer) WriteSummaryOffset(s *SummaryOffset) error {
	msglen := 1 + 8 + 8
	w.ensureSized(msglen)
	w.msg[0] = byte(s.GroupOpcode)
	offset := 1
	offset += putUint64(w.msg[offset:], s.GroupStart)
	offset += putUint64(w.msg[offset:], s.GroupLength)
	_, err := w.writeRecord(w.w, OpSummaryOffset, w.msg[:offset])
	return err
}

// WriteDataEnd writes a data end record to the output. A Data End record
// indicates the end of the data section.
func (w *Writer) WriteDataEnd(e *DataEnd) error {
	msglen := 4
	w.ensureSized(msglen)
	offset := putUint32(w.msg, e.DataSectionCRC)
	_, err := w.writeRecord(w.w, OpDataEnd, w.msg[:offset])
	return err
}

func (w *Writer) flushActiveChunk() error {
	err := w.compressedWriter.Close()
	if err != nil {
		return err
	}
	crc := w.compressedWriter.CRC()
	compressedlen := w.compressed.Len()
	uncompressedlen := w.compressedWriter.Size()
	msglen := 8 + 8 + 8 + 4 + 4 + len(w.compression) + 8 + compressedlen
	chunkStartOffset := w.w.Size()
	start := w.currentChunkStartTime
	end := w.currentChunkEndTime

	// when writing a chunk, we don't go through writerecord to avoid needing to
	// materialize the compressed data again. Instead, write the leading bytes
	// then copy from the compressed data buffer.
	recordlen := 1 + 8 + msglen
	if len(w.chunk) < recordlen {
		w.chunk = make([]byte, recordlen*2)
	}
	offset, err := putByte(w.chunk, byte(OpChunk))
	if err != nil {
		return err
	}
	offset += putUint64(w.chunk[offset:], uint64(msglen))
	offset += putUint64(w.chunk[offset:], start)
	offset += putUint64(w.chunk[offset:], end)
	offset += putUint64(w.chunk[offset:], uint64(uncompressedlen))
	offset += putUint32(w.chunk[offset:], crc)
	offset += putPrefixedString(w.chunk[offset:], string(w.compression))
	offset += putUint64(w.chunk[offset:], uint64(w.compressed.Len()))
	offset += copy(w.chunk[offset:recordlen], w.compressed.Bytes())
	_, err = w.w.Write(w.chunk[:offset])
	if err != nil {
		return err
	}
	w.compressed.Reset()
	w.compressedWriter.Reset(w.compressed)
	w.compressedWriter.ResetSize()
	w.compressedWriter.ResetCRC()

	messageIndexOffsets := make(map[uint16]uint64)
	messageIndexStart := w.w.Size()
	for _, chanID := range w.channelIDs {
		if messageIndex, ok := w.messageIndexes[chanID]; ok {
			messageIndex.Insort()
			messageIndexOffsets[messageIndex.ChannelID] = w.w.Size()
			err = w.WriteMessageIndex(messageIndex)
			if err != nil {
				return err
			}
		}
	}
	messageIndexEnd := w.w.Size()
	messageIndexLength := messageIndexEnd - messageIndexStart
	w.ChunkIndexes = append(w.ChunkIndexes, &ChunkIndex{
		MessageStartTime:    w.currentChunkStartTime,
		MessageEndTime:      w.currentChunkEndTime,
		ChunkStartOffset:    chunkStartOffset,
		ChunkLength:         messageIndexStart - chunkStartOffset,
		MessageIndexOffsets: messageIndexOffsets,
		MessageIndexLength:  messageIndexLength,
		Compression:         w.compression,
		CompressedSize:      uint64(compressedlen),
		UncompressedSize:    uint64(uncompressedlen),
	})
	for _, idx := range w.messageIndexes {
		idx.Reset()
	}
	w.Statistics.ChunkCount++
	w.currentChunkStartTime = math.MaxUint64
	w.currentChunkEndTime = 0
	return nil
}

func makePrefixedMap(m map[string]string) []byte {
	maplen := 0
	mapkeys := make([]string, 0, len(m))
	for k, v := range m {
		maplen += 4 + len(k) + 4 + len(v)
		mapkeys = append(mapkeys, k)
	}
	sort.Strings(mapkeys)
	buf := make([]byte, maplen+4)
	offset := putUint32(buf, uint32(maplen))
	for _, k := range mapkeys {
		v := m[k]
		offset += putPrefixedString(buf[offset:], k)
		offset += putPrefixedString(buf[offset:], v)
	}
	return buf
}

func (w *Writer) writeChunkIndex(idx *ChunkIndex) error {
	messageIndexLength := len(idx.MessageIndexOffsets) * (2 + 8)
	msglen := 8 + 8 + 8 + 8 + 4 + messageIndexLength + 8 + 4 + len(idx.Compression) + 8 + 8
	w.ensureSized(msglen)
	offset := putUint64(w.msg, idx.MessageStartTime)
	offset += putUint64(w.msg[offset:], idx.MessageEndTime)
	offset += putUint64(w.msg[offset:], idx.ChunkStartOffset)
	offset += putUint64(w.msg[offset:], idx.ChunkLength)
	offset += putUint32(w.msg[offset:], uint32(messageIndexLength))
	for _, chanID := range w.channelIDs {
		if v, ok := idx.MessageIndexOffsets[chanID]; ok {
			offset += putUint16(w.msg[offset:], chanID)
			offset += putUint64(w.msg[offset:], v)
		}
	}
	offset += putUint64(w.msg[offset:], idx.MessageIndexLength)
	offset += putPrefixedString(w.msg[offset:], string(idx.Compression))
	offset += putUint64(w.msg[offset:], idx.CompressedSize)
	offset += putUint64(w.msg[offset:], idx.UncompressedSize)
	_, err := w.writeRecord(w.w, OpChunkIndex, w.msg[:offset])
	return err
}

func (w *Writer) ensureSized(n int) {
	if len(w.msg) < n {
		w.msg = make([]byte, 2*n)
	}
}

// Close the writer by closing the active chunk and writing the summary section.
func (w *Writer) Close() error {
	if w.chunked {
		err := w.flushActiveChunk()
		if err != nil {
			return fmt.Errorf("failed to flush active chunks: %w", err)
		}
	}
	w.chunked = false

	err := w.WriteDataEnd(&DataEnd{
		DataSectionCRC: 0,
	})
	if err != nil {
		return fmt.Errorf("failed to write data end: %w", err)
	}

	// summary section
	channelInfoOffset := w.w.Size()
	for _, chanID := range w.channelIDs {
		if channelInfo, ok := w.channels[chanID]; ok {
			err := w.WriteChannel(channelInfo)
			if err != nil {
				return fmt.Errorf("failed to write channel info: %w", err)
			}
		}
	}
	schemaOffset := w.w.Size()
	for _, schemaID := range w.schemaIDs {
		if schema, ok := w.schemas[schemaID]; ok {
			err := w.WriteSchema(schema)
			if err != nil {
				return fmt.Errorf("failed to write schema: %w", err)
			}
		}
	}
	chunkIndexOffset := w.w.Size()
	for _, chunkIndex := range w.ChunkIndexes {
		err := w.writeChunkIndex(chunkIndex)
		if err != nil {
			return fmt.Errorf("failed to write chunk index: %w", err)
		}
	}
	attachmentIndexOffset := w.w.Size()
	for _, attachmentIndex := range w.AttachmentIndexes {
		err := w.WriteAttachmentIndex(attachmentIndex)
		if err != nil {
			return fmt.Errorf("failed to write attachment index: %w", err)
		}
	}
	statisticsOffset := w.w.Size()
	err = w.WriteStatistics(w.Statistics)
	if err != nil {
		return fmt.Errorf("failed to write statistics: %w", err)
	}

	// summary offset section
	summaryOffsetStart := w.w.Size()

	if len(w.channels) > 0 {
		err = w.WriteSummaryOffset(&SummaryOffset{
			GroupOpcode: OpChannel,
			GroupStart:  channelInfoOffset,
			GroupLength: schemaOffset - channelInfoOffset,
		})
		if err != nil {
			return fmt.Errorf("failed to write summary offset: %w", err)
		}
	}
	if len(w.schemas) > 0 {
		err = w.WriteSummaryOffset(&SummaryOffset{
			GroupOpcode: OpSchema,
			GroupStart:  schemaOffset,
			GroupLength: chunkIndexOffset - channelInfoOffset,
		})
		if err != nil {
			return fmt.Errorf("failed to write summary offset: %w", err)
		}
	}
	if len(w.ChunkIndexes) > 0 {
		err = w.WriteSummaryOffset(&SummaryOffset{
			GroupOpcode: OpChunkIndex,
			GroupStart:  chunkIndexOffset,
			GroupLength: attachmentIndexOffset - chunkIndexOffset,
		})
		if err != nil {
			return fmt.Errorf("failed to write chunk index summary offset: %w", err)
		}
	}
	if len(w.AttachmentIndexes) > 0 {
		err = w.WriteSummaryOffset(&SummaryOffset{
			GroupOpcode: OpAttachmentIndex,
			GroupStart:  attachmentIndexOffset,
			GroupLength: statisticsOffset - attachmentIndexOffset,
		})
		if err != nil {
			return fmt.Errorf("failed to write attachment index summary offset: %w", err)
		}
	}
	if w.Statistics != nil {
		err = w.WriteSummaryOffset(&SummaryOffset{
			GroupOpcode: OpStatistics,
			GroupStart:  statisticsOffset,
			GroupLength: statisticsOffset - attachmentIndexOffset,
		})
		if err != nil {
			return fmt.Errorf("failed to write statistics summary offset: %w", err)
		}
	}
	err = w.WriteFooter(&Footer{
		SummaryStart:       channelInfoOffset,
		SummaryOffsetStart: summaryOffsetStart,
		SummaryCRC:         0,
	})
	if err != nil {
		return fmt.Errorf("failed to write footer record: %w", err)
	}
	_, err = w.w.Write(Magic)
	if err != nil {
		return fmt.Errorf("failed to write closing magic: %w", err)
	}
	return nil
}

func (w *Writer) writeRecord(writer io.Writer, op OpCode, data []byte) (int, error) {
	c := 0
	w.buf[0] = byte(op)
	putUint64(w.buf[1:], uint64(len(data)))
	n, err := writer.Write(w.buf[:9])
	c += n
	if err != nil {
		return c, err
	}
	n, err = writer.Write(data)
	c += n
	if err != nil {
		return c, err
	}
	return c, nil
}

// WriterOptions are options for the MCAP Writer.
type WriterOptions struct {
	// IncludeCRC specifies whether to compute CRC checksums in the output.
	IncludeCRC bool
	// Chunked specifies whether the file should be chunk-compressed.
	Chunked bool
	// ChunkSize specifies a target chunk size for compressed chunks. This size
	// may be exceeded, for instance in the case of oversized messages.
	ChunkSize int64
	// Compression indicates the compression format to use for chunk compression.
	Compression CompressionFormat
}

// NewWriter returns a new MCAP writer.
func NewWriter(w io.Writer, opts *WriterOptions) (*Writer, error) {
	writer := NewWriteSizer(w)
	if _, err := writer.Write(Magic); err != nil {
		return nil, err
	}
	compressed := bytes.Buffer{}
	var compressedWriter *CountingCRCWriter
	if opts.Chunked {
		switch opts.Compression {
		case CompressionZSTD:
			zw, err := zstd.NewWriter(&compressed, zstd.WithEncoderLevel(zstd.SpeedFastest))
			if err != nil {
				return nil, err
			}
			compressedWriter = NewCountingCRCWriter(zw, opts.IncludeCRC)
		case CompressionLZ4:
			compressedWriter = NewCountingCRCWriter(lz4.NewWriter(&compressed), opts.IncludeCRC)
		case CompressionNone:
			compressedWriter = NewCountingCRCWriter(bufCloser{&compressed}, opts.IncludeCRC)
		default:
			return nil, fmt.Errorf("unsupported compression")
		}
		if opts.ChunkSize == 0 {
			opts.ChunkSize = 1024 * 1024
		}
	}
	return &Writer{
		w:                     writer,
		buf:                   make([]byte, 32),
		channels:              make(map[uint16]*Channel),
		schemas:               make(map[uint16]*Schema),
		messageIndexes:        make(map[uint16]*MessageIndex),
		uncompressed:          &bytes.Buffer{},
		compressed:            &compressed,
		chunksize:             opts.ChunkSize,
		chunked:               opts.Chunked,
		compression:           opts.Compression,
		compressedWriter:      compressedWriter,
		includeCRC:            opts.IncludeCRC,
		currentChunkStartTime: math.MaxUint64,
		currentChunkEndTime:   0,
		Statistics: &Statistics{
			ChannelMessageCounts: make(map[uint16]uint64),
			MessageStartTime:     math.MaxUint64,
			MessageEndTime:       0,
		},
	}, nil
}
