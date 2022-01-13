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
	channels          map[uint16]*ChannelInfo
	w                 *WriteSizer
	buf8              []byte
	msg               []byte
	chunked           bool
	includeCRC        bool
	uncompressed      *bytes.Buffer
	chunksize         int64
	compressed        *bytes.Buffer
	compression       CompressionFormat
	stats             *Statistics
	messageIndexes    map[uint16]*MessageIndex
	chunkIndexes      []*ChunkIndex
	attachmentIndexes []*AttachmentIndex

	compressedWriter *CountingCRCWriter
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
	msglen := 8 + 4 + 4 + len(w.compression) + compressedlen
	chunkStartOffset := w.w.Size()

	// when writing a chunk, we don't go through writerecord to avoid needing to
	// materialize the compressed data again. Instead, write the leading bytes
	// then copy from the compressed data buffer.
	buf := make([]byte, 1+8+8+4+4+len(w.compression))
	offset := putByte(buf, byte(OpChunk))
	offset += putUint64(buf[offset:], uint64(msglen))
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

	// TODO change spec no chunk index record if no messages in the chunk
	msgidxOffsets := make(map[uint16]uint64)
	var start, end uint64
	start = math.MaxInt64
	end = 0
	messageIndexStart := w.w.Size()
	for _, msgidx := range w.messageIndexes {
		// TODO evaluate custom sort for mostly sorted input
		sort.Slice(msgidx.Records, func(i, j int) bool {
			return msgidx.Records[i].Timestamp < msgidx.Records[j].Timestamp
		})

		if first := msgidx.Records[0].Timestamp; first < start {
			start = first
		}
		if last := msgidx.Records[len(msgidx.Records)-1].Timestamp; last > end {
			end = last
		}
		msgidxOffsets[msgidx.ChannelID] = w.w.Size()
		err = w.WriteMessageIndex(msgidx)
		if err != nil {
			return err
		}
	}
	messageIndexEnd := w.w.Size()
	messageIndexLength := messageIndexEnd - messageIndexStart
	w.chunkIndexes = append(w.chunkIndexes, &ChunkIndex{
		StartTime:           start,
		EndTime:             end,
		ChunkOffset:         chunkStartOffset,
		MessageIndexOffsets: msgidxOffsets,
		MessageIndexLength:  messageIndexLength,
		Compression:         w.compression,
		CompressedSize:      uint64(compressedlen),
		UncompressedSize:    uint64(uncompressedlen),
	})
	for k := range w.messageIndexes {
		delete(w.messageIndexes, k)
	}
	w.stats.ChunkCount++
	return nil
}

func (w *Writer) WriteMessage(m *Message) error {
	if w.channels[m.ChannelID] == nil {
		return fmt.Errorf("unrecognized channel %d", m.ChannelID)
	}
	msglen := 2 + 4 + 8 + 8 + len(m.Data)
	if len(w.msg) < msglen {
		w.msg = make([]byte, 2*msglen)
	}
	offset := putUint16(w.msg, m.ChannelID)
	offset += putUint32(w.msg[offset:], m.Sequence)
	offset += putUint64(w.msg[offset:], uint64(m.PublishTime))
	offset += putUint64(w.msg[offset:], uint64(m.RecordTime))
	offset += copy(w.msg[offset:], m.Data)
	w.stats.ChannelStats[m.ChannelID]++
	w.stats.MessageCount++
	if w.chunked {

		// TODO preallocate or maybe fancy structure. These could be conserved
		// across chunks too, which might work ok assuming similar numbers of
		// messages/chan/chunk.

		idx, ok := w.messageIndexes[m.ChannelID]
		if !ok {
			idx = &MessageIndex{
				ChannelID: m.ChannelID,
				Count:     0,
				Records:   nil,
			}
			w.messageIndexes[m.ChannelID] = idx
		}
		idx.Records = append(idx.Records, MessageIndexRecord{m.RecordTime, uint64(w.compressedWriter.Size())})
		idx.Count++
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
	msglen := 2 + 4 + 4 + datalen + 4
	if len(w.msg) < msglen {
		w.msg = make([]byte, 2*msglen)
	}
	offset := putUint16(w.msg, idx.ChannelID)
	offset += putUint32(w.msg[offset:], idx.Count)
	offset += putUint32(w.msg[offset:], uint32(datalen))
	for _, v := range idx.Records {
		offset += putUint64(w.msg[offset:], uint64(v.Timestamp))
		offset += putUint64(w.msg[offset:], uint64(v.Offset))
	}
	crc := crc32.ChecksumIEEE(w.msg[:offset])
	offset += putUint32(w.msg[offset:], crc)
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

func (w *Writer) WriteHeader(profile string, library string, metadata map[string]string) error {
	data := makePrefixedMap(metadata)
	buf := make([]byte, len(data)+len(profile)+4+len(library)+4)
	offset := putPrefixedString(buf, profile)
	offset += putPrefixedString(buf[offset:], library)
	offset += copy(buf[offset:], data)
	_, err := w.writeRecord(w.w, OpHeader, buf[:offset])
	return err
}

func (w *Writer) WriteChannelInfo(c *ChannelInfo) error {
	userdata := makePrefixedMap(c.UserData)
	msglen := 2 + 4 + len(c.TopicName) + 4 + len(c.Encoding) + 4 + len(c.SchemaName) + 4 + len(c.Schema) + len(userdata) + 4
	if len(w.msg) < msglen {
		w.msg = make([]byte, 2*msglen)
	}
	offset := putUint16(w.msg, c.ChannelID)
	offset += putPrefixedString(w.msg[offset:], c.TopicName)
	offset += putPrefixedString(w.msg[offset:], c.Encoding)
	offset += putPrefixedString(w.msg[offset:], c.SchemaName)
	offset += putPrefixedBytes(w.msg[offset:], c.Schema)
	offset += copy(w.msg[offset:], userdata)
	crc := crc32.ChecksumIEEE(w.msg[:offset])
	offset += putUint32(w.msg[offset:], crc)
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
		w.stats.ChannelCount++
		w.channels[c.ChannelID] = c
	}
	return nil
}

func (w *Writer) WriteAttachment(a *Attachment) error {
	msglen := 4 + len(a.Name) + 8 + 4 + len(a.ContentType) + 8 + len(a.Data) + 4
	if len(w.msg) < msglen {
		w.msg = make([]byte, 2*msglen)
	}
	offset := putPrefixedString(w.msg, a.Name)
	offset += putUint64(w.msg[offset:], a.RecordTime)
	offset += putPrefixedString(w.msg[offset:], a.ContentType)
	offset += putUint64(w.msg[offset:], uint64(len(a.Data)))
	offset += copy(w.msg[offset:], a.Data)
	attachmentOffset := w.w.Size()
	_, err := w.writeRecord(w.w, OpAttachment, w.msg[:offset])
	if err != nil {
		return err
	}
	w.attachmentIndexes = append(w.attachmentIndexes, &AttachmentIndex{
		RecordTime:     a.RecordTime,
		AttachmentSize: uint64(len(a.Data)),
		Name:           a.Name,
		ContentType:    a.ContentType,
		Offset:         attachmentOffset,
	})
	w.stats.AttachmentCount++
	return nil
}

func (w *Writer) WriteAttachmentIndex(idx *AttachmentIndex) error {
	msglen := 8 + 8 + 4 + len(idx.Name) + 4 + len(idx.ContentType) + 8
	if len(w.msg) < msglen {
		w.msg = make([]byte, 2*msglen)
	}
	offset := putUint64(w.msg, idx.RecordTime)
	offset += putUint64(w.msg[offset:], idx.AttachmentSize)
	offset += putPrefixedString(w.msg[offset:], idx.Name)
	offset += putPrefixedString(w.msg[offset:], idx.ContentType)
	offset += putUint64(w.msg[offset:], idx.Offset)
	_, err := w.writeRecord(w.w, OpAttachmentIndex, w.msg[:offset])
	return err
}

func (w *Writer) writeChunkIndex(idx *ChunkIndex) error {
	msgidxlen := len(idx.MessageIndexOffsets) * (2 + 8)
	msglen := 8 + 8 + 8 + 4 + msgidxlen + 8 + 4 + len(idx.Compression) + 8 + 8 + 4
	if len(w.msg) < msglen {
		w.msg = make([]byte, 2*msglen)
	}
	offset := putUint64(w.msg, idx.StartTime)
	offset += putUint64(w.msg[offset:], idx.EndTime)
	offset += putUint64(w.msg[offset:], idx.ChunkOffset)

	offset += putUint32(w.msg[offset:], uint32(msgidxlen))
	for k, v := range idx.MessageIndexOffsets {
		offset += putUint16(w.msg[offset:], k)
		offset += putUint64(w.msg[offset:], v)
	}
	offset += putUint64(w.msg[offset:], idx.MessageIndexLength)
	offset += putPrefixedString(w.msg[offset:], string(idx.Compression))
	offset += putUint64(w.msg[offset:], idx.CompressedSize)
	offset += putUint64(w.msg[offset:], idx.UncompressedSize)

	crc := crc32.ChecksumIEEE(w.msg[:offset])
	offset += putUint32(w.msg[offset:], crc)
	_, err := w.writeRecord(w.w, OpChunkIndex, w.msg[:offset])
	return err
}

func (w *Writer) WriteStatistics(s *Statistics) error {
	msglen := 8 + 4 + 4 + 4 + len(s.ChannelStats)*(2+8)
	if len(w.msg) < msglen {
		w.msg = make([]byte, 2*msglen)
	}
	offset := putUint64(w.msg, s.MessageCount)
	offset += putUint32(w.msg[offset:], s.ChannelCount)
	offset += putUint32(w.msg[offset:], s.AttachmentCount)
	offset += putUint32(w.msg[offset:], s.ChunkCount)
	offset += putUint32(w.msg[offset:], uint32(len(s.ChannelStats)*(2+8)))
	for k, v := range s.ChannelStats {
		offset += putUint16(w.msg[offset:], k)
		offset += putUint64(w.msg[offset:], v)
	}
	_, err := w.writeRecord(w.w, OpStatistics, w.msg[:offset])
	return err
}

func (w *Writer) WriteFooter(f *Footer) error {
	msglen := 8 + 4
	if len(w.msg) < msglen {
		w.msg = make([]byte, 2*msglen)
	}
	offset := putUint64(w.msg, f.IndexOffset)
	offset += putUint32(w.msg[offset:], f.IndexCRC)
	_, err := w.writeRecord(w.w, OpFooter, w.msg[:offset])
	return err
}

func (w *Writer) Close() error {
	if w.chunked {
		err := w.writeChunk()
		if err != nil {
			return err
		}
	}
	indexOffset := w.w.Size()
	w.chunked = false
	for _, channelInfo := range w.channels {
		err := w.WriteChannelInfo(channelInfo)
		if err != nil {
			return err
		}
	}
	for _, chunkidx := range w.chunkIndexes {
		err := w.writeChunkIndex(chunkidx)
		if err != nil {
			return err
		}
	}
	for _, attachmentidx := range w.attachmentIndexes {
		err := w.WriteAttachmentIndex(attachmentidx)
		if err != nil {
			return err
		}
	}
	err := w.WriteStatistics(w.stats)
	if err != nil {
		return err
	}
	err = w.WriteFooter(&Footer{
		IndexOffset: indexOffset,
		IndexCRC:    0,
	})
	if err != nil {
		return err
	}
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
	_, err := writer.Write(Magic)
	if err != nil {
		return nil, err
	}
	compressed := bytes.Buffer{}
	var compressedWriter *CountingCRCWriter
	if opts.Compression != "" {
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
		w:                writer,
		buf8:             make([]byte, 8),
		channels:         make(map[uint16]*ChannelInfo),
		messageIndexes:   make(map[uint16]*MessageIndex),
		uncompressed:     &bytes.Buffer{},
		compressed:       &compressed,
		chunksize:        opts.ChunkSize,
		chunked:          opts.Chunked,
		compression:      opts.Compression,
		compressedWriter: compressedWriter,
		includeCRC:       opts.IncludeCRC,
		stats: &Statistics{
			ChannelStats: make(map[uint16]uint64),
		},
	}, nil
}
