package mcap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"sort"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type messageIndexEntry struct {
	offset    uint64
	timestamp uint64
	channelID uint16
}

func newMessageIndexEntry(offset uint64, timestamp uint64, channelID uint16) messageIndexEntry {
	return messageIndexEntry{
		offset:    offset,
		timestamp: timestamp,
		channelID: channelID,
	}
}

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
	// MetadataIndexes created over the course of the recording.
	MetadataIndexes []*MetadataIndex

	currentMessageIndex []messageIndexEntry

	channelIDs        []uint16
	schemaIDs         []uint16
	channels          map[uint16]*Channel
	schemas           map[uint16]*Schema
	messageIndexes    map[uint16]*MessageIndex
	w                 *writeSizer
	buf               []byte
	msg               []byte
	chunk             []byte
	uncompressed      *bytes.Buffer
	compressed        *bytes.Buffer
	compressedWriter  resettableWriteCloser
	uncompressedChunk *bytes.Buffer

	currentChunkStartTime uint64
	currentChunkEndTime   uint64
	chunkCRC              hash.Hash32
	opts                  *WriterOptions
}

// WriteHeader writes a header record to the output.
func (w *Writer) WriteHeader(header *Header) error {
	var library string
	if !w.opts.OverrideLibrary {
		library = fmt.Sprintf("mcap go #%s", Version())
		if header.Library != "" {
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
	var summaryCrc uint32
	if w.opts.IncludeCRC {
		summaryCrc = w.w.Checksum()
	}
	offset += putUint32(w.msg[offset:], summaryCrc)
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
	if w.opts.Chunked {
		_, err = w.writeRecord(w.uncompressedChunk, OpSchema, w.msg[:offset])
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
	if w.opts.Chunked {
		_, err := w.writeRecord(w.uncompressedChunk, OpChannel, w.msg[:offset])
		if err != nil {
			return err
		}
	} else {
		_, err := w.writeRecord(w.w, OpChannel, w.msg[:offset])
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

func (w *Writer) currentChunkSize() int64 {
	return int64(w.uncompressedChunk.Len())
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
	if w.opts.Chunked {
		if !w.opts.SkipMessageIndexing {
			w.currentMessageIndex = append(
				w.currentMessageIndex,
				newMessageIndexEntry(uint64(w.currentChunkSize()), m.LogTime, m.ChannelID))
		}
		_, err := w.writeRecord(w.uncompressedChunk, OpMessage, w.msg[:offset])
		if err != nil {
			return err
		}
		if m.LogTime > w.currentChunkEndTime {
			w.currentChunkEndTime = m.LogTime
		}
		if m.LogTime < w.currentChunkStartTime {
			w.currentChunkStartTime = m.LogTime
		}
		if w.currentChunkSize() > w.opts.ChunkSize {
			err := w.flushActiveChunk()
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
	if w.opts.SkipAttachmentIndex {
		return nil
	}
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

func u64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// swapSlices swaps the intervals of buf defined by leftstart/leftend and
// rightstart/rightend. The intervals are assumed to be nonoverlapping, and
// bounds-checked. The argument tmp is a scratch buffer; if set to nil, or if
// the length is insufficient, one will be allocated. The return value is the
// length of scratch space used.
func swapSlices(tmp []byte, buf []byte, leftstart, leftend, rightstart, rightend int) []byte {
	leftLen := leftend - leftstart
	rightLen := rightend - rightstart
	scratchlen := max(leftLen, rightLen)
	if len(tmp) < scratchlen {
		tmp = make([]byte, scratchlen)
	}
	scratch := tmp[:scratchlen]
	switch {
	case leftLen > rightLen:
		// copy the left message into a temporary buffer
		copy(scratch, buf[leftstart:leftend])
		// copy the right message into the beginning of the space
		// previously occupied by the left message
		copy(
			buf[leftstart:],
			buf[rightstart:rightend],
		)
		// shift the bytes after the left message leftward by
		// leftLen - rightLen
		copy(
			buf[leftstart+rightLen:],
			buf[leftend:rightstart],
		)
		// place the left hand message at the old right offset,
		// translated by leftLen-rightLen
		copy(buf[rightstart-leftLen+rightLen:], scratch)
	case leftLen < rightLen:
		// copy the right message into a temporary buffer
		copy(scratch, buf[rightstart:rightend])
		// copy the left message into the end of the space
		// previously occupied by the right message
		copy(
			buf[rightend-leftLen:],
			buf[leftstart:leftend],
		)
		// shift bytes from left end to the old start of right, forward by rightLen - leftLen
		copy(
			buf[leftend+rightLen-leftLen:rightstart+rightLen-leftLen],
			buf[leftend:rightstart],
		)
		// place the right hand message at the old left offset
		copy(buf[leftstart:], scratch)
	case leftLen == rightLen:
		// directly swap the messages through scratch
		copy(scratch, buf[leftstart:])
		copy(buf[leftstart:], buf[rightstart:rightstart+rightLen])
		copy(buf[rightstart:rightstart+rightLen], scratch)
	}
	return tmp
}

// sortChunk sorts the input chunk, and the provided index, using the provided
// index on (timestamp, offset). Uses an insertion sort under the assumption the
// input chunk is mostly sorted already and disorderings are usually localized.
func sortChunk(tmp []byte, chunk []byte, index []messageIndexEntry) {
	i := 1
	for i < len(index) {
		j := i
		for j > 0 &&
			(index[j-1].timestamp > index[j].timestamp ||
				(index[j-1].timestamp == index[j].timestamp &&
					index[j-1].offset > index[j].offset)) {
			right := index[j]
			left := index[j-1]
			// swap entries in the index
			index[j-1], index[j] = index[j], index[j-1]

			// swap the corresponding records in the chunk
			leftRecordLen := u64(chunk[left.offset+1:])
			rightRecordLen := u64(chunk[right.offset+1:])
			leftLen := 1 + 8 + leftRecordLen
			rightLen := 1 + 8 + rightRecordLen
			tmp = swapSlices(
				tmp,
				chunk,
				int(left.offset),
				int(left.offset+leftLen),
				int(right.offset),
				int(right.offset+rightLen),
			)
			// recompute offsets for the swapped entries
			index[j-1].offset = left.offset
			switch {
			case leftLen == rightLen:
				index[j].offset = right.offset
			case rightLen > leftLen:
				index[j].offset = right.offset + (rightLen - leftLen)
			case leftLen > rightLen:
				index[j].offset = right.offset - (leftLen - rightLen)
			}
			j--
		}
		i++
	}
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
	uncompressedLen := w.currentChunkSize()
	if uncompressedLen == 0 {
		return nil
	}

	_, err := w.chunkCRC.Write(w.uncompressedChunk.Bytes())
	if err != nil {
		return fmt.Errorf("failed to compute chunk CRC: %w", err)
	}
	crc := w.chunkCRC.Sum32()

	uncompressedChunk := w.uncompressedChunk.Bytes()
	if w.opts.SortChunkMessages {
		sortChunk(w.msg, uncompressedChunk, w.currentMessageIndex)
	}

	// compress the data
	_, err = w.compressedWriter.Write(uncompressedChunk)
	if err != nil {
		return fmt.Errorf("failed to compress chunk: %w", err)
	}

	// flush any remaining data
	err = w.compressedWriter.Close()
	if err != nil {
		return fmt.Errorf("failed to close chunk: %w", err)
	}

	compressedlen := w.compressed.Len()
	msglen := 8 + 8 + 8 + 4 + 4 + len(w.opts.Compression) + 8 + compressedlen
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
	offset += putUint64(w.chunk[offset:], uint64(uncompressedLen))
	offset += putUint32(w.chunk[offset:], crc)
	offset += putPrefixedString(w.chunk[offset:], string(w.opts.Compression))
	offset += putUint64(w.chunk[offset:], uint64(w.compressed.Len()))
	offset += copy(w.chunk[offset:recordlen], w.compressed.Bytes())
	_, err = w.w.Write(w.chunk[:offset])
	if err != nil {
		return err
	}
	w.compressed.Reset()
	w.compressedWriter.Reset(w.compressed)
	w.uncompressedChunk.Reset()
	w.chunkCRC.Reset()
	chunkEndOffset := w.w.Size()
	messageIndexOffsets := make(map[uint16]uint64)
	if !w.opts.SkipMessageIndexing {
		for i := range w.currentMessageIndex {
			channelID := w.currentMessageIndex[i].channelID
			idx, ok := w.messageIndexes[channelID]
			if !ok {
				idx = &MessageIndex{ChannelID: channelID, Records: nil}
				w.messageIndexes[channelID] = idx
			}
			idx.Add(w.currentMessageIndex[i].timestamp, w.currentMessageIndex[i].offset)
		}
		for _, chanID := range w.channelIDs {
			if messageIndex, ok := w.messageIndexes[chanID]; ok {
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
	if w.currentChunkStartTime != math.MaxUint64 {
		chunkStart = w.currentChunkStartTime
	}
	w.ChunkIndexes = append(w.ChunkIndexes, &ChunkIndex{
		MessageStartTime:    chunkStart,
		MessageEndTime:      w.currentChunkEndTime,
		ChunkStartOffset:    chunkStartOffset,
		ChunkLength:         chunkEndOffset - chunkStartOffset,
		MessageIndexOffsets: messageIndexOffsets,
		MessageIndexLength:  messageIndexLength,
		Compression:         w.opts.Compression,
		CompressedSize:      uint64(compressedlen),
		UncompressedSize:    uint64(uncompressedLen),
	})
	for _, idx := range w.messageIndexes {
		idx.Reset()
	}
	w.currentMessageIndex = w.currentMessageIndex[:0]
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
		err := w.flushActiveChunk()
		if err != nil {
			return fmt.Errorf("failed to flush active chunks: %w", err)
		}
	}
	w.opts.Chunked = false

	err := w.WriteDataEnd(&DataEnd{
		DataSectionCRC: 0,
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

	// SortChunkMessages causes the messages in the chunks and chunk indexes
	// produced by the writer to be chronologically ordered.
	SortChunkMessages bool
}

// NewWriter returns a new MCAP writer.
func NewWriter(w io.Writer, opts *WriterOptions) (*Writer, error) {
	writer := newWriteSizer(w)
	if _, err := writer.Write(Magic); err != nil {
		return nil, err
	}
	uncompressedChunk := &bytes.Buffer{}
	compressed := &bytes.Buffer{}
	var err error
	var compressedWriter resettableWriteCloser
	if opts.Chunked {
		if opts.ChunkSize == 0 {
			opts.ChunkSize = 1024 * 1024
		}
		switch opts.Compression {
		case CompressionLZ4:
			compressedWriter = lz4.NewWriter(compressed)
		case CompressionZSTD:
			compressedWriter, err = zstd.NewWriter(compressed, zstd.WithEncoderLevel(zstd.SpeedFastest))
			if err != nil {
				return nil, fmt.Errorf("failed to build zstd writer: %w", err)
			}
		case CompressionNone:
			compressedWriter = bufCloser{compressed}
		}
	}
	return &Writer{
		w:                     writer,
		buf:                   make([]byte, 32),
		channels:              make(map[uint16]*Channel),
		schemas:               make(map[uint16]*Schema),
		messageIndexes:        make(map[uint16]*MessageIndex),
		uncompressed:          &bytes.Buffer{},
		uncompressedChunk:     uncompressedChunk,
		compressed:            compressed,
		compressedWriter:      compressedWriter,
		currentChunkStartTime: math.MaxUint64,
		currentChunkEndTime:   0,
		Statistics: &Statistics{
			ChannelMessageCounts: make(map[uint16]uint64),
			MessageStartTime:     0,
			MessageEndTime:       0,
		},
		opts:                opts,
		chunkCRC:            crc32.NewIEEE(),
		currentMessageIndex: []messageIndexEntry{},
	}, nil
}
