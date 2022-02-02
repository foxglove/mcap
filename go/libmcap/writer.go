package libmcap

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sort"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type Writer struct {
	Statistics        *Statistics
	MessageIndexes    map[uint16]*MessageIndex
	ChunkIndexes      []*ChunkIndex
	AttachmentIndexes []*AttachmentIndex

	channels         map[uint16]*ChannelInfo
	w                *WriteSizer
	buf8             []byte
	msg              []byte
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

func (w *Writer) writeRecord(writer io.Writer, op OpCode, data []byte) (int, error) {
	c, err := writer.Write([]byte{byte(op)})
	if err != nil {
		return c, err
	}
	putUint64(w.buf8, uint64(len(data)))
	n, err := writer.Write(w.buf8)
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

func (w *Writer) writeChunk() error {
	err := w.compressedWriter.Close()
	if err != nil {
		return err
	}
	crc := w.compressedWriter.CRC()
	compressedlen := w.compressed.Len()
	uncompressedlen := w.compressedWriter.Size()
	msglen := 8 + 8 + 8 + 4 + 4 + len(w.compression) + compressedlen
	chunkStartOffset := w.w.Size()
	start := w.currentChunkStartTime
	end := w.currentChunkEndTime

	// when writing a chunk, we don't go through writerecord to avoid needing to
	// materialize the compressed data again. Instead, write the leading bytes
	// then copy from the compressed data buffer.
	buf := make([]byte, 1+8+msglen)
	offset := putByte(buf, byte(OpChunk))
	offset += putUint64(buf[offset:], uint64(msglen))
	offset += putUint64(buf[offset:], start)
	offset += putUint64(buf[offset:], end)
	offset += putUint64(buf[offset:], uint64(uncompressedlen))
	offset += putUint32(buf[offset:], crc)
	offset += putPrefixedString(buf[offset:], string(w.compression))
	_, err = w.w.Write(buf[:offset])
	if err != nil {
		return err
	}
	// copy the compressed data buffer, then reset it
	_, err = io.Copy(w.w, w.compressed)
	if err != nil {
		return err
	}
	w.compressed.Reset()
	w.compressedWriter.Reset(w.compressed)
	w.compressedWriter.ResetSize()
	w.compressedWriter.ResetCRC()

	msgidxOffsets := make(map[uint16]uint64)
	messageIndexStart := w.w.Size()
	for _, msgidx := range w.MessageIndexes {
		sort.Slice(msgidx.Records, func(i, j int) bool {
			return msgidx.Records[i].Timestamp < msgidx.Records[j].Timestamp
		})
		msgidxOffsets[msgidx.ChannelID] = w.w.Size()
		err = w.WriteMessageIndex(msgidx)
		if err != nil {
			return err
		}
	}
	messageIndexEnd := w.w.Size()
	messageIndexLength := messageIndexEnd - messageIndexStart
	w.ChunkIndexes = append(w.ChunkIndexes, &ChunkIndex{
		StartTime:           w.currentChunkStartTime,
		EndTime:             w.currentChunkEndTime,
		ChunkStartOffset:    chunkStartOffset,
		MessageIndexOffsets: msgidxOffsets,
		MessageIndexLength:  messageIndexLength,
		Compression:         w.compression,
		CompressedSize:      uint64(compressedlen),
		UncompressedSize:    uint64(uncompressedlen),
	})
	for k := range w.MessageIndexes {
		delete(w.MessageIndexes, k)
	}
	w.Statistics.ChunkCount++
	w.currentChunkStartTime = math.MaxUint64
	w.currentChunkEndTime = 0
	return nil
}

func (w *Writer) WriteMessage(m *Message) error {
	if w.channels[m.ChannelID] == nil {
		return fmt.Errorf("unrecognized channel %d", m.ChannelID)
	}
	msglen := 2 + 4 + 8 + 8 + len(m.Data)
	w.ensureSized(msglen)
	offset := putUint16(w.msg, m.ChannelID)
	offset += putUint32(w.msg[offset:], m.Sequence)
	offset += putUint64(w.msg[offset:], m.PublishTime)
	offset += putUint64(w.msg[offset:], m.RecordTime)
	offset += copy(w.msg[offset:], m.Data)
	w.Statistics.ChannelMessageCounts[m.ChannelID]++
	w.Statistics.MessageCount++
	if w.chunked {
		// TODO preallocate or maybe fancy structure. These could be conserved
		// across chunks too, which might work ok assuming similar numbers of
		// messages/chan/chunk.
		idx, ok := w.MessageIndexes[m.ChannelID]
		if !ok {
			idx = &MessageIndex{
				ChannelID: m.ChannelID,
				Records:   nil,
			}
			w.MessageIndexes[m.ChannelID] = idx
		}
		idx.Records = append(idx.Records, MessageIndexEntry{m.RecordTime, uint64(w.compressedWriter.Size())})
		_, err := w.writeRecord(w.compressedWriter, OpMessage, w.msg[:offset])
		if err != nil {
			return err
		}
		if w.compressedWriter.Size() > w.chunksize {
			err := w.writeChunk()
			if err != nil {
				return err
			}
		}
		if m.RecordTime > w.currentChunkEndTime {
			w.currentChunkEndTime = m.RecordTime
		}
		if m.RecordTime < w.currentChunkStartTime {
			w.currentChunkStartTime = m.RecordTime
		}
		return nil
	}
	_, err := w.writeRecord(w.w, OpMessage, w.msg[:offset])
	if err != nil {
		return err
	}
	return nil
}

func (w *Writer) WriteMessageIndex(idx *MessageIndex) error {
	datalen := len(idx.Records) * (8 + 8)
	msglen := 2 + 4 + datalen
	w.ensureSized(msglen)
	offset := putUint16(w.msg, idx.ChannelID)
	offset += putUint32(w.msg[offset:], uint32(datalen))
	for _, v := range idx.Records {
		offset += putUint64(w.msg[offset:], v.Timestamp)
		offset += putUint64(w.msg[offset:], v.Offset)
	}
	_, err := w.writeRecord(w.w, OpMessageIndex, w.msg[:offset])
	return err
}

func makePrefixedMap(m map[string]string) []byte {
	maplen := 0
	for k, v := range m {
		maplen += 4 + len(k) + 4 + len(v)
	}
	buf := make([]byte, maplen+4)
	offset := putUint32(buf, uint32(maplen))
	for k, v := range m {
		offset += putPrefixedString(buf[offset:], k)
		offset += putPrefixedString(buf[offset:], v)
	}
	return buf
}

func (w *Writer) WriteHeader(header *Header) error {
	buf := make([]byte, 4+len(header.Profile)+4+len(header.Library))
	offset := putPrefixedString(buf, header.Profile)
	offset += putPrefixedString(buf[offset:], header.Library)
	_, err := w.writeRecord(w.w, OpHeader, buf[:offset])
	return err
}

func (w *Writer) WriteChannelInfo(c *ChannelInfo) error {
	userdata := makePrefixedMap(c.Metadata)
	msglen := (2 +
		4 + len(c.TopicName) +
		4 + len(c.MessageEncoding) +
		4 + len(c.SchemaEncoding) +
		4 + len(c.Schema) +
		4 + len(c.SchemaName) +
		len(userdata))
	w.ensureSized(msglen)
	offset := putUint16(w.msg, c.ChannelID)
	offset += putPrefixedString(w.msg[offset:], c.TopicName)
	offset += putPrefixedString(w.msg[offset:], c.MessageEncoding)
	offset += putPrefixedString(w.msg[offset:], c.SchemaEncoding)
	offset += putPrefixedBytes(w.msg[offset:], c.Schema)
	offset += putPrefixedString(w.msg[offset:], c.SchemaName)
	offset += copy(w.msg[offset:], userdata)
	var err error
	if w.chunked {
		_, err = w.writeRecord(w.compressedWriter, OpChannelInfo, w.msg[:offset])
		if err != nil {
			return err
		}
	} else {
		_, err = w.writeRecord(w.w, OpChannelInfo, w.msg[:offset])
		if err != nil {
			return err
		}
	}
	if _, ok := w.channels[c.ChannelID]; !ok {
		w.Statistics.ChannelCount++
		w.channels[c.ChannelID] = c
	}
	return nil
}

func (w *Writer) WriteAttachment(a *Attachment) error {
	msglen := 4 + len(a.Name) + 8 + 4 + len(a.ContentType) + 8 + len(a.Data) + 4
	w.ensureSized(msglen)
	offset := putPrefixedString(w.msg, a.Name)
	offset += putUint64(w.msg[offset:], a.CreatedAt)
	offset += putUint64(w.msg[offset:], a.RecordTime)
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
		RecordTime:  a.RecordTime,
		DataSize:    uint64(len(a.Data)),
		Name:        a.Name,
		ContentType: a.ContentType,
	})
	w.Statistics.AttachmentCount++
	return nil
}

func (w *Writer) WriteAttachmentIndex(idx *AttachmentIndex) error {
	msglen := 8 + 8 + 8 + 8 + 4 + len(idx.Name) + 4 + len(idx.ContentType)
	w.ensureSized(msglen)
	offset := putUint64(w.msg, idx.Offset)
	offset += putUint64(w.msg[offset:], idx.Length)
	offset += putUint64(w.msg[offset:], idx.RecordTime)
	offset += putUint64(w.msg[offset:], idx.DataSize)
	offset += putPrefixedString(w.msg[offset:], idx.Name)
	offset += putPrefixedString(w.msg[offset:], idx.ContentType)
	_, err := w.writeRecord(w.w, OpAttachmentIndex, w.msg[:offset])
	return err
}

func (w *Writer) writeChunkIndex(idx *ChunkIndex) error {
	msgidxlen := len(idx.MessageIndexOffsets) * (2 + 8)
	msglen := 8 + 8 + 8 + 8 + 4 + msgidxlen + 8 + 4 + len(idx.Compression) + 8 + 8
	w.ensureSized(msglen)
	offset := putUint64(w.msg, idx.StartTime)
	offset += putUint64(w.msg[offset:], idx.EndTime)
	offset += putUint64(w.msg[offset:], idx.ChunkStartOffset)
	offset += putUint64(w.msg[offset:], idx.ChunkLength)
	offset += putUint32(w.msg[offset:], uint32(msgidxlen))
	for k, v := range idx.MessageIndexOffsets {
		offset += putUint16(w.msg[offset:], k)
		offset += putUint64(w.msg[offset:], v)
	}
	offset += putUint64(w.msg[offset:], idx.MessageIndexLength)
	offset += putPrefixedString(w.msg[offset:], string(idx.Compression))
	offset += putUint64(w.msg[offset:], idx.CompressedSize)
	offset += putUint64(w.msg[offset:], idx.UncompressedSize)
	_, err := w.writeRecord(w.w, OpChunkIndex, w.msg[:offset])
	return err
}

func (w *Writer) WriteStatistics(s *Statistics) error {
	msglen := 8 + 4 + 4 + 4 + len(s.ChannelMessageCounts)*(2+8)
	w.ensureSized(msglen)
	offset := putUint64(w.msg, s.MessageCount)
	offset += putUint32(w.msg[offset:], s.ChannelCount)
	offset += putUint32(w.msg[offset:], s.AttachmentCount)
	offset += putUint32(w.msg[offset:], s.ChunkCount)
	offset += putUint32(w.msg[offset:], uint32(len(s.ChannelMessageCounts)*(2+8)))
	for k, v := range s.ChannelMessageCounts {
		offset += putUint16(w.msg[offset:], k)
		offset += putUint64(w.msg[offset:], v)
	}
	_, err := w.writeRecord(w.w, OpStatistics, w.msg[:offset])
	return err
}

func (w *Writer) ensureSized(n int) {
	if len(w.msg) < n {
		w.msg = make([]byte, 2*n)
	}
}

func (w *Writer) WriteFooter(f *Footer) error {
	msglen := 8 + 8 + 4
	w.ensureSized(msglen)
	offset := putUint64(w.msg, f.SummaryStart)
	offset += putUint64(w.msg[offset:], f.SummaryOffsetStart)
	offset += putUint32(w.msg[offset:], f.SummaryCRC)
	_, err := w.writeRecord(w.w, OpFooter, w.msg[:offset])
	return err
}

func (w *Writer) WriteMetadata(m *Metadata) error {
	data := makePrefixedMap(m.Metadata)
	msglen := 4 + len(m.Name) + 4 + len(data)
	w.ensureSized(msglen)
	offset := putPrefixedString(w.msg, m.Name)
	offset += copy(w.msg[offset:], data)
	_, err := w.writeRecord(w.w, OpMetadata, w.msg[:offset])
	return err
}

func (w *Writer) WriteMetadataIndex(idx *MetadataIndex) error {
	msglen := 8 + 8 + 4 + len(idx.Name)
	w.ensureSized(msglen)
	offset := putUint64(w.msg, idx.Offset)
	offset += putUint64(w.msg[offset:], idx.Length)
	offset += putPrefixedString(w.msg[offset:], idx.Name)
	_, err := w.writeRecord(w.w, OpMetadataIndex, w.msg[:offset])
	return err
}

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

func (w *Writer) Close() error {
	if w.chunked {
		err := w.writeChunk()
		if err != nil {
			return err
		}
	}

	w.chunked = false

	// summary section
	channelInfoOffset := w.w.Size()
	for _, channelInfo := range w.channels {
		err := w.WriteChannelInfo(channelInfo)
		if err != nil {
			return err
		}
	}
	chunkIndexOffset := w.w.Size()
	for _, chunkidx := range w.ChunkIndexes {
		err := w.writeChunkIndex(chunkidx)
		if err != nil {
			return err
		}
	}
	attachmentIndexOffset := w.w.Size()
	for _, attachmentidx := range w.AttachmentIndexes {
		err := w.WriteAttachmentIndex(attachmentidx)
		if err != nil {
			return err
		}
	}
	statisticsOffset := w.w.Size()
	err := w.WriteStatistics(w.Statistics)
	if err != nil {
		return err
	}

	// summary offset section
	summaryOffsetStart := w.w.Size()

	if len(w.channels) > 0 {
		err = w.WriteSummaryOffset(&SummaryOffset{
			GroupOpcode: OpChannelInfo,
			GroupStart:  channelInfoOffset,
			GroupLength: chunkIndexOffset - channelInfoOffset,
		})
		if err != nil {
			return err
		}
	}
	if len(w.ChunkIndexes) > 0 {
		err = w.WriteSummaryOffset(&SummaryOffset{
			GroupOpcode: OpChunkIndex,
			GroupStart:  chunkIndexOffset,
			GroupLength: attachmentIndexOffset - chunkIndexOffset,
		})
		if err != nil {
			return err
		}
	}
	if len(w.AttachmentIndexes) > 0 {
		err = w.WriteSummaryOffset(&SummaryOffset{
			GroupOpcode: OpAttachmentIndex,
			GroupStart:  attachmentIndexOffset,
			GroupLength: statisticsOffset - attachmentIndexOffset,
		})
		if err != nil {
			return err
		}
	}
	if w.Statistics != nil {
		err = w.WriteSummaryOffset(&SummaryOffset{
			GroupOpcode: OpStatistics,
			GroupStart:  statisticsOffset,
			GroupLength: statisticsOffset - attachmentIndexOffset,
		})
		if err != nil {
			return err
		}
	}

	// footer
	err = w.WriteFooter(&Footer{
		SummaryStart:       channelInfoOffset,
		SummaryOffsetStart: summaryOffsetStart,
		SummaryCRC:         0,
	})
	if err != nil {
		return err
	}
	// magic
	_, err = w.w.Write(Magic)
	if err != nil {
		return err
	}
	return nil
}

type WriterOptions struct {
	IncludeCRC  bool
	Chunked     bool
	ChunkSize   int64
	Compression CompressionFormat
}

func NewWriter(w io.Writer, opts *WriterOptions) (*Writer, error) {
	writer := NewWriteSizer(w)
	if _, err := writer.Write(Magic); err != nil {
		return nil, err
	}
	compressed := bytes.Buffer{}
	var compressedWriter *CountingCRCWriter
	if opts.Chunked {
		switch opts.Compression {
		case CompressionLZ4:
			compressedWriter = NewCountingCRCWriter(lz4.NewWriter(&compressed), opts.IncludeCRC)
		case CompressionZSTD:
			zw, err := zstd.NewWriter(&compressed)
			if err != nil {
				return nil, err
			}
			compressedWriter = NewCountingCRCWriter(zw, opts.IncludeCRC)
		case CompressionNone:
			compressedWriter = NewCountingCRCWriter(BufCloser{&compressed}, opts.IncludeCRC)
		default:
			return nil, fmt.Errorf("unsupported compression")
		}
		if opts.ChunkSize == 0 {
			opts.ChunkSize = 1024 * 1024
		}
	}
	return &Writer{
		w:                     writer,
		buf8:                  make([]byte, 8),
		channels:              make(map[uint16]*ChannelInfo),
		MessageIndexes:        make(map[uint16]*MessageIndex),
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
		},
	}, nil
}
