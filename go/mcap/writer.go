package mcap

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
)

// ErrUnknownSchema is returned when a schema ID is not known to the writer.
var ErrUnknownSchema = errors.New("unknown schema")

// ErrAttachmentDataSizeIncorrect is returned when the length of a written
// attachment does not match the length supplied.
var ErrAttachmentDataSizeIncorrect = errors.New("attachment content length incorrect")

// ColumnSelector defines an interface for types which can determine which column of chunks
// a record should be written to.
type ColumnSelector interface {
	ColumnForSchema(*Schema) (int, error)
	ColumnForChannel(*Channel) (int, error)
	ColumnForMessage(*Message) (int, error)
}

// Writer is a writer for the MCAP format.
type Writer struct {
	// Statistics collected over the course of the recording.
	Statistics *Statistics
	// ChunkIndexes created over the course of the recording.
	ChunkIndexes []*ChunkIndex
	// AttachmentIndexes created over the course of the recording.
	AttachmentIndexes []*AttachmentIndex
	// MetadataIndexes created over the course of the recording.
	MetadataIndexes []*MetadataIndex

	channelIDs   []uint16
	schemaIDs    []uint16
	channels     map[uint16]*Channel
	schemas      map[uint16]*Schema
	w            *writeSizer
	buf          []byte
	msg          []byte
	chunk        []byte
	chunkWriters []*ChunkWriter

	opts *WriterOptions

	closed bool
}

// WriteHeader writes a header record to the output.
func (w *Writer) WriteHeader(header *Header) error {
	var library string
	if !w.opts.OverrideLibrary {
		library = fmt.Sprintf("mcap go %s", Version)
		if header.Library != "" && header.Library != library {
			library += "; " + header.Library
		}
	} else {
		library = header.Library
	}
	msglen := 4 + len(header.Profile) + 4 + len(library)
	w.ensureSized(msglen)
	offset := putPrefixedString(w.msg, header.Profile)
	offset += putPrefixedString(w.msg[offset:], library)
	_, err := w.writeRecord(w.w, OpHeader, w.msg[:offset])
	return err
}

// Offset returns the current offset of the writer, or the size of the written
// file if called after Close().
func (w *Writer) Offset() uint64 {
	return w.w.Size()
}

// WriteFooter writes a footer record to the output. A Footer record contains end-of-file
// information. It must be the last record in the file. Readers using the index to read the file
// will begin with by reading the footer and trailing magic.
//
// If opts.IncludeCRC is enabled, the CRC is expected to have been reset after the DataEnd record
// was written.
func (w *Writer) WriteFooter(f *Footer) error {
	msglen := 8 + 8 + 4
	w.ensureSized(1 + 8 + msglen)
	w.msg[0] = byte(OpFooter)
	offset := 1
	offset += putUint64(w.msg[offset:], uint64(msglen))
	offset += putUint64(w.msg[offset:], f.SummaryStart)
	offset += putUint64(w.msg[offset:], f.SummaryOffsetStart)
	_, err := w.w.Write(w.msg[:offset])
	if err != nil {
		return err
	}
	offset += putUint32(w.msg[offset:], w.w.Checksum())
	_, err = w.w.Write(w.msg[offset-4 : offset])
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
	if w.opts.Chunked && !w.closed {
		var chunkWriter *ChunkWriter
		if w.opts.ColumnSelector != nil {
			columnIndex, err := w.opts.ColumnSelector.ColumnForSchema(s)
			if err != nil {
				return err
			}
			chunkWriter, err = w.chunkWriterForColumnIndex(columnIndex)
			if err != nil {
				return err
			}
		} else {
			chunkWriter = w.chunkWriters[0]
		}
		_, err = w.writeRecord(chunkWriter, OpSchema, w.msg[:offset])
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
	if w.opts.Chunked && !w.closed {
		var chunkWriter *ChunkWriter
		if w.opts.ColumnSelector != nil {
			columnIndex, err := w.opts.ColumnSelector.ColumnForChannel(c)
			if err != nil {
				return err
			}
			chunkWriter, err = w.chunkWriterForColumnIndex(columnIndex)
			if err != nil {
				return err
			}
		} else {
			chunkWriter = w.chunkWriters[0]
		}
		_, err = w.writeRecord(chunkWriter, OpChannel, w.msg[:offset])
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

func (w *Writer) chunkWriterForColumnIndex(idx int) (*ChunkWriter, error) {
	for len(w.chunkWriters) < (idx + 1) {
		chunkWriter, err := newChunkWriter(w.opts.Compression, w.opts.IncludeCRC)
		if err != nil {
			return nil, err
		}
		w.chunkWriters = append(w.chunkWriters, chunkWriter)
	}
	return w.chunkWriters[idx], nil
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
	if w.opts.Chunked && !w.closed {
		var chunkWriter *ChunkWriter
		if w.opts.ColumnSelector != nil {
			columnIndex, err := w.opts.ColumnSelector.ColumnForMessage(m)
			if err != nil {
				return err
			}
			chunkWriter, err = w.chunkWriterForColumnIndex(columnIndex)
			if err != nil {
				return err
			}
		} else {
			chunkWriter = w.chunkWriters[0]
		}
		idx, ok := chunkWriter.MessageIndexes[m.ChannelID]
		if !ok {
			idx = &MessageIndex{
				ChannelID: m.ChannelID,
				Records:   nil,
			}
			chunkWriter.MessageIndexes[m.ChannelID] = idx
		}
		idx.Add(m.LogTime, uint64(chunkWriter.UncompressedLen()))
		_, err := w.writeRecord(chunkWriter, OpMessage, w.msg[:offset])
		if err != nil {
			return err
		}
		if m.LogTime > chunkWriter.ChunkEndTime {
			chunkWriter.ChunkEndTime = m.LogTime
		}
		if m.LogTime < chunkWriter.ChunkStartTime {
			chunkWriter.ChunkStartTime = m.LogTime
		}
		if chunkWriter.UncompressedLen() > w.opts.ChunkSize {
			err := w.flushChunk(chunkWriter)
			if err != nil {
				return err
			}
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
	if m.LogTime < w.Statistics.MessageStartTime || w.Statistics.MessageStartTime == 0 {
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
	bufferLen := 1 + // opcode
		8 + // record length
		8 + // log time
		8 + // create time
		4 + len(a.Name) + // name
		4 + len(a.MediaType) + // media type
		8 // content length
	w.ensureSized(bufferLen)

	offset, err := putByte(w.msg, byte(OpAttachment))
	if err != nil {
		return err
	}
	offset += putUint64(w.msg[offset:], uint64(bufferLen)+a.DataSize+4-9)
	offset += putUint64(w.msg[offset:], a.LogTime)
	offset += putUint64(w.msg[offset:], a.CreateTime)
	offset += putPrefixedString(w.msg[offset:], a.Name)
	offset += putPrefixedString(w.msg[offset:], a.MediaType)
	offset += putUint64(w.msg[offset:], a.DataSize)

	attachmentOffset := w.w.Size()
	// leading 9 bytes not included in CRC
	_, err = w.w.Write(w.msg[:9])
	if err != nil {
		return err
	}
	crcWriter := newCRCWriter(w.w)
	_, err = crcWriter.Write(w.msg[9:offset])
	if err != nil {
		return fmt.Errorf("failed to write attachment metadata: %w", err)
	}
	bytesWritten, err := io.Copy(crcWriter, a.Data)
	if err != nil {
		return fmt.Errorf("failed to write attachment data: %w", err)
	}
	if uint64(bytesWritten) != a.DataSize {
		return ErrAttachmentDataSizeIncorrect
	}
	putUint32(w.msg[:4], crcWriter.Checksum())
	_, err = w.w.Write(w.msg[:4])
	if err != nil {
		return fmt.Errorf("failed to write attachment crc: %w", err)
	}
	w.AttachmentIndexes = append(w.AttachmentIndexes, &AttachmentIndex{
		Offset:     attachmentOffset,
		Length:     uint64(bufferLen) + a.DataSize + 4,
		LogTime:    a.LogTime,
		CreateTime: a.CreateTime,
		DataSize:   a.DataSize,
		Name:       a.Name,
		MediaType:  a.MediaType,
	})
	w.Statistics.AttachmentCount++
	return nil
}

// WriteAttachmentIndex writes an attachment index record to the output. An
// Attachment Index record contains the location of an attachment in the file.
// An Attachment Index record exists for every Attachment record in the file.
func (w *Writer) WriteAttachmentIndex(idx *AttachmentIndex) error {
	if w.opts.SkipAttachmentIndex {
		return nil
	}
	msglen := 8 + 8 + 8 + 8 + 8 + 4 + len(idx.Name) + 4 + len(idx.MediaType)
	w.ensureSized(msglen)
	offset := putUint64(w.msg, idx.Offset)
	offset += putUint64(w.msg[offset:], idx.Length)
	offset += putUint64(w.msg[offset:], idx.LogTime)
	offset += putUint64(w.msg[offset:], idx.CreateTime)
	offset += putUint64(w.msg[offset:], idx.DataSize)
	offset += putPrefixedString(w.msg[offset:], idx.Name)
	offset += putPrefixedString(w.msg[offset:], idx.MediaType)
	_, err := w.writeRecord(w.w, OpAttachmentIndex, w.msg[:offset])
	return err
}

// WriteStatistics writes a statistics record to the output. A Statistics record
// contains summary information about the recorded data. The statistics record
// is optional, but the file should contain at most one.
func (w *Writer) WriteStatistics(s *Statistics) error {
	if w.opts.SkipStatistics {
		return nil
	}
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
	metadataOffset := w.w.Size()
	c, err := w.writeRecord(w.w, OpMetadata, w.msg[:offset])
	if err != nil {
		return err
	}
	w.MetadataIndexes = append(w.MetadataIndexes, &MetadataIndex{
		Offset: metadataOffset,
		Length: uint64(c),
		Name:   m.Name,
	})
	w.Statistics.MetadataCount++
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
	if w.opts.SkipSummaryOffsets {
		return nil
	}
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

func (w *Writer) flushChunk(chunkWriter *ChunkWriter) error {
	if chunkWriter.UncompressedLen() == 0 {
		return nil
	}

	err := chunkWriter.Close()
	if err != nil {
		return err
	}
	compressedlen := chunkWriter.CompressedLen()
	uncompressedlen := chunkWriter.UncompressedLen()
	chunkStartOffset := w.w.Size()

	// when writing a chunk, we don't go through writerecord to avoid needing to
	// materialize the compressed data again. Instead, write the leading bytes
	// then copy from the compressed data buffer.
	recordlen := 1 + 8 + chunkWriter.SerializedLen()
	if len(w.chunk) < recordlen {
		w.chunk = make([]byte, recordlen*2)
	}
	offset, err := putByte(w.chunk, byte(OpChunk))
	if err != nil {
		return err
	}
	offset += putUint64(w.chunk[offset:], uint64(chunkWriter.SerializedLen()))
	serializedlen, err := chunkWriter.SerializeTo(w.chunk[offset:])
	offset += serializedlen
	if err != nil {
		return err
	}
	_, err = w.w.Write(w.chunk[:offset])
	if err != nil {
		return err
	}
	chunkEndOffset := w.w.Size()

	// message indexes
	messageIndexOffsets := make(map[uint16]uint64)
	if !w.opts.SkipMessageIndexing {
		for _, chanID := range w.channelIDs {
			if messageIndex, ok := chunkWriter.MessageIndexes[chanID]; ok {
				messageIndex.Insort()
				messageIndexOffsets[messageIndex.ChannelID] = w.w.Size()
				err = w.WriteMessageIndex(messageIndex)
				if err != nil {
					return err
				}
			}
		}
	}

	messageIndexEnd := w.w.Size()
	messageIndexLength := messageIndexEnd - chunkEndOffset
	var chunkStart uint64
	if chunkWriter.ChunkStartTime != math.MaxUint64 {
		chunkStart = chunkWriter.ChunkStartTime
	}
	w.ChunkIndexes = append(w.ChunkIndexes, &ChunkIndex{
		MessageStartTime:    chunkStart,
		MessageEndTime:      chunkWriter.ChunkEndTime,
		ChunkStartOffset:    chunkStartOffset,
		ChunkLength:         chunkEndOffset - chunkStartOffset,
		MessageIndexOffsets: messageIndexOffsets,
		MessageIndexLength:  messageIndexLength,
		Compression:         w.opts.Compression,
		CompressedSize:      uint64(compressedlen),
		UncompressedSize:    uint64(uncompressedlen),
	})
	chunkWriter.Reset()
	for _, idx := range chunkWriter.MessageIndexes {
		idx.Reset()
	}
	w.Statistics.ChunkCount++
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

func (w *Writer) writeSummarySection() ([]*SummaryOffset, error) {
	offsets := []*SummaryOffset{}
	if !w.opts.SkipRepeatedSchemas {
		if len(w.schemas) > 0 {
			schemaOffset := w.w.Size()
			for _, schemaID := range w.schemaIDs {
				if schema, ok := w.schemas[schemaID]; ok {
					err := w.WriteSchema(schema)
					if err != nil {
						return offsets, fmt.Errorf("failed to write schema: %w", err)
					}
				}
			}
			offsets = append(offsets, &SummaryOffset{
				GroupOpcode: OpSchema,
				GroupStart:  schemaOffset,
				GroupLength: w.w.Size() - schemaOffset,
			})
		}
	}
	if !w.opts.SkipRepeatedChannelInfos {
		if len(w.channels) > 0 {
			channelInfoOffset := w.w.Size()
			for _, chanID := range w.channelIDs {
				if channelInfo, ok := w.channels[chanID]; ok {
					err := w.WriteChannel(channelInfo)
					if err != nil {
						return offsets, fmt.Errorf("failed to write channel info: %w", err)
					}
				}
			}
			offsets = append(offsets, &SummaryOffset{
				GroupOpcode: OpChannel,
				GroupStart:  channelInfoOffset,
				GroupLength: w.w.Size() - channelInfoOffset,
			})
		}
	}
	if !w.opts.SkipStatistics {
		statisticsOffset := w.w.Size()
		err := w.WriteStatistics(w.Statistics)
		if err != nil {
			return offsets, fmt.Errorf("failed to write statistics: %w", err)
		}
		offsets = append(offsets, &SummaryOffset{
			GroupOpcode: OpStatistics,
			GroupStart:  statisticsOffset,
			GroupLength: w.w.Size() - statisticsOffset,
		})
	}
	if !w.opts.SkipChunkIndex {
		if len(w.ChunkIndexes) > 0 {
			chunkIndexOffset := w.w.Size()
			for _, chunkIndex := range w.ChunkIndexes {
				err := w.writeChunkIndex(chunkIndex)
				if err != nil {
					return offsets, fmt.Errorf("failed to write chunk index: %w", err)
				}
			}
			offsets = append(offsets, &SummaryOffset{
				GroupOpcode: OpChunkIndex,
				GroupStart:  chunkIndexOffset,
				GroupLength: w.w.Size() - chunkIndexOffset,
			})
		}
	}
	if !w.opts.SkipAttachmentIndex {
		if len(w.AttachmentIndexes) > 0 {
			attachmentIndexOffset := w.w.Size()
			for _, attachmentIndex := range w.AttachmentIndexes {
				err := w.WriteAttachmentIndex(attachmentIndex)
				if err != nil {
					return offsets, fmt.Errorf("failed to write attachment index: %w", err)
				}
			}
			offsets = append(offsets, &SummaryOffset{
				GroupOpcode: OpAttachmentIndex,
				GroupStart:  attachmentIndexOffset,
				GroupLength: w.w.Size() - attachmentIndexOffset,
			})
		}
	}
	if !w.opts.SkipMetadataIndex {
		if len(w.MetadataIndexes) > 0 {
			metadataIndexOffset := w.w.Size()
			for _, metadataIndex := range w.MetadataIndexes {
				err := w.WriteMetadataIndex(metadataIndex)
				if err != nil {
					return offsets, fmt.Errorf("failed to write metadata index: %w", err)
				}
			}
			offsets = append(offsets, &SummaryOffset{
				GroupOpcode: OpMetadataIndex,
				GroupStart:  metadataIndexOffset,
				GroupLength: w.w.Size() - metadataIndexOffset,
			})
		}
	}

	return offsets, nil
}

// Close the writer by closing the active chunk and writing the summary section.
func (w *Writer) Close() error {
	if w.opts.Chunked {
		for i := range w.chunkWriters {
			err := w.flushChunk(w.chunkWriters[i])
			if err != nil {
				return fmt.Errorf("failed to flush active chunks: %w", err)
			}
		}
	}
	w.closed = true
	err := w.WriteDataEnd(&DataEnd{
		DataSectionCRC: w.w.Checksum(),
	})
	if err != nil {
		return fmt.Errorf("failed to write data end: %w", err)
	}

	// summary section
	w.w.ResetCRC() // reset CRC to begin computing summaryCrc
	summarySectionStart := w.w.Size()
	summaryOffsets, err := w.writeSummarySection()
	if err != nil {
		return fmt.Errorf("failed to write summary section: %w", err)
	}
	if len(summaryOffsets) == 0 {
		summarySectionStart = 0
	}
	var summaryOffsetStart uint64
	if !w.opts.SkipSummaryOffsets {
		summaryOffsetStart = w.w.Size()
		for _, summaryOffset := range summaryOffsets {
			err := w.WriteSummaryOffset(summaryOffset)
			if err != nil {
				return fmt.Errorf("failed to write summary offset: %w", err)
			}
		}
	}
	err = w.WriteFooter(&Footer{
		SummaryStart:       summarySectionStart,
		SummaryOffsetStart: summaryOffsetStart,
		// SummaryCrc is calculated in WriteFooter
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

	// SkipMessageIndexing skips the message and chunk indexes for a chunked
	// file.
	SkipMessageIndexing bool

	// SkipStatistics skips the statistics accounting.
	SkipStatistics bool

	// SkipRepeatedSchemas skips the schemas repeated at the end of the file
	SkipRepeatedSchemas bool

	// SkipRepeatedChannelInfos skips the channel infos repeated at the end of
	// the file
	SkipRepeatedChannelInfos bool

	// SkipAttachmentIndex skips indexing for attachments
	SkipAttachmentIndex bool

	// SkipMetadataIndex skips metadata index records.
	SkipMetadataIndex bool

	// SkipChunkIndex skips chunk index records.
	SkipChunkIndex bool

	// SkipSummaryOffsets skips summary offset records.
	SkipSummaryOffsets bool

	// OverrideLibrary causes the default header library to be overridden, not
	// appended to.
	OverrideLibrary bool

	// ColumnSelector optionally determines which column of chunks Schemas, Channels, and Messages
	// should be written to.
	ColumnSelector ColumnSelector
}

// NewWriter returns a new MCAP writer.
func NewWriter(w io.Writer, opts *WriterOptions) (*Writer, error) {
	writer := newWriteSizer(w, opts.IncludeCRC)
	if _, err := writer.Write(Magic); err != nil {
		return nil, err
	}
	if opts.Chunked {
		if opts.ChunkSize == 0 {
			opts.ChunkSize = 1024 * 1024
		}
	}
	firstChunkWriter, err := newChunkWriter(opts.Compression, opts.IncludeCRC)
	if err != nil {
		return nil, err
	}
	return &Writer{
		w:            writer,
		buf:          make([]byte, 32),
		channels:     make(map[uint16]*Channel),
		schemas:      make(map[uint16]*Schema),
		chunkWriters: []*ChunkWriter{firstChunkWriter},
		Statistics: &Statistics{
			ChannelMessageCounts: make(map[uint16]uint64),
			MessageStartTime:     0,
			MessageEndTime:       0,
		},
		opts: opts,
	}, nil
}
