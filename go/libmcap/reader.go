package libmcap

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

func readPrefixedString(data []byte, offset int) (string, int, error) {
	if len(data[offset:]) < 4 {
		return "", 0, io.ErrShortBuffer
	}
	length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	if len(data[offset+4:]) < length {
		return "", 0, io.ErrShortBuffer
	}
	return string(data[offset+4 : offset+length+4]), offset + 4 + length, nil
}

func readPrefixedBytes(data []byte, offset int) ([]byte, int, error) {
	if len(data[offset:]) < 4 {
		return nil, 0, io.ErrShortBuffer
	}
	length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	if len(data[offset+4:]) < length {
		return nil, 0, io.ErrShortBuffer
	}
	return data[offset+4 : offset+length+4], offset + 4 + length, nil
}

func parseChunk(buf []byte) (*Chunk, error) {
	uncompressedSize, offset := getUint64(buf, 0)
	uncompressedCRC, offset := getUint32(buf, offset)
	compression, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
	}
	records := buf[offset:]
	return &Chunk{
		UncompressedSize: uncompressedSize,
		UncompressedCRC:  uncompressedCRC,
		Compression:      compression,
		Records:          records,
	}, nil
}

func parseMessageIndex(buf []byte) *MessageIndex {
	channelID, offset := getUint16(buf, 0)
	count, offset := getUint32(buf, offset)
	_, offset = getUint32(buf, offset)
	var recordTime uint64
	var recordOffset uint64
	records := make([]MessageIndexRecord, count)
	for i := range records {
		recordTime, offset = getUint64(buf, offset)
		recordOffset, offset = getUint64(buf, offset)
		records[i] = MessageIndexRecord{
			Timestamp: recordTime,
			Offset:    recordOffset,
		}
	}
	crc, offset := getUint32(buf, offset)
	return &MessageIndex{
		ChannelID: channelID,
		Count:     count,
		Records:   records,
		CRC:       crc,
	}
}

func parseAttachmentIndex(buf []byte) (*AttachmentIndex, error) {
	recordTime, offset := getUint64(buf, 0)
	dataSize, offset := getUint64(buf, offset)
	name, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
	}
	contentType, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
	}
	attachmentOffset, offset := getUint64(buf, offset)
	return &AttachmentIndex{
		RecordTime:     recordTime,
		AttachmentSize: dataSize,
		Name:           name,
		ContentType:    contentType,
		Offset:         attachmentOffset,
	}, nil
}

func parseMessage(buf []byte) *Message {
	channelID, offset := getUint16(buf, 0)
	sequence, offset := getUint32(buf, offset)
	publishTime, offset := getUint64(buf, offset)
	recordTime, offset := getUint64(buf, offset)
	data := buf[offset:]
	return &Message{
		ChannelID:   channelID,
		Sequence:    sequence,
		RecordTime:  recordTime,
		PublishTime: publishTime,
		Data:        data,
	}
}

func parseChunkIndex(buf []byte) (*ChunkIndex, error) {
	startTime, offset := getUint64(buf, 0)
	endTime, offset := getUint64(buf, offset)
	chunkOffset, offset := getUint64(buf, offset)
	msgIndexLen, offset := getUint32(buf, offset)
	messageIndexOffsets := make(map[uint16]uint64)
	var chanID uint16
	var indexOffset uint64
	inset := 0
	for inset < int(msgIndexLen) {
		chanID, inset = getUint16(buf[offset:], inset)
		indexOffset, inset = getUint64(buf[offset:], inset)
		messageIndexOffsets[chanID] = indexOffset
	}
	offset += inset
	msgIndexLength, offset := getUint64(buf, offset)
	compression, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
	}
	compressedSize, offset := getUint64(buf, offset)
	uncompressedSize, offset := getUint64(buf, offset)
	return &ChunkIndex{
		StartTime:           startTime,
		EndTime:             endTime,
		ChunkOffset:         chunkOffset,
		MessageIndexOffsets: messageIndexOffsets,
		MessageIndexLength:  msgIndexLength,
		Compression:         CompressionFormat(compression),
		CompressedSize:      compressedSize,
		UncompressedSize:    uncompressedSize,
	}, nil
}

func parseChannelInfo(buf []byte) (*ChannelInfo, error) {
	channelID, offset := getUint16(buf, 0)
	topicName, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
	}
	encoding, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
	}
	schemaName, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
	}
	schema, offset, err := readPrefixedBytes(buf, offset)
	if err != nil {
		return nil, err
	}
	userdata, offset, err := readPrefixedMap(buf, offset)
	if err != nil {
		return nil, err
	}
	return &ChannelInfo{
		ChannelID:  channelID,
		TopicName:  topicName,
		Encoding:   encoding,
		SchemaName: schemaName,
		Schema:     schema,
		UserData:   userdata,
	}, nil
}

func parseStatisticsRecord(buf []byte) *Statistics {
	messageCount, offset := getUint64(buf, 0)
	channelCount, offset := getUint32(buf, offset)
	attachmentCount, offset := getUint32(buf, offset)
	chunkCount, offset := getUint32(buf, offset)

	// TODO this is not actually necessary, since the bytes are at the end of
	// the record
	_, offset = getUint32(buf, offset)
	var chanID uint16
	var channelMessageCount uint64
	channelStats := make(map[uint16]uint64)
	for offset < len(buf) {
		chanID, offset = getUint16(buf, offset)
		channelMessageCount, offset = getUint64(buf, offset)
		channelStats[chanID] = channelMessageCount
	}
	return &Statistics{
		MessageCount:    messageCount,
		ChannelCount:    channelCount,
		AttachmentCount: attachmentCount,
		ChunkCount:      chunkCount,
		ChannelStats:    channelStats,
	}
}

func readPrefixedMap(data []byte, offset int) (map[string]string, int, error) {
	var err error
	var key, value string
	var inset int
	m := make(map[string]string)
	maplen, offset := getUint32(data, offset)
	for uint32(offset+inset) < uint32(offset)+maplen {
		key, inset, err = readPrefixedString(data[offset:], inset)
		if err != nil {
			return nil, 0, err
		}
		value, inset, err = readPrefixedString(data[offset:], inset)
		if err != nil {
			return nil, 0, err
		}
		m[key] = value
	}
	return m, offset + inset, nil
}

type Reader struct {
	l                 *lexer
	r                 io.Reader
	rs                io.ReadSeeker
	channels          map[uint16]*ChannelInfo
	statistics        *Statistics
	chunkIndexes      []*ChunkIndex
	attachmentIndexes []*AttachmentIndex
}

type MessageIterator interface {
	Next() (*ChannelInfo, *Message, error)
}

func (r *Reader) unindexedIterator(topics []string, start uint64, end uint64) *unindexedMessageIterator {
	topicMap := make(map[string]bool)
	for _, topic := range topics {
		topicMap[topic] = true
	}
	r.l.emitChunks = false
	return &unindexedMessageIterator{
		lexer:    r.l,
		channels: make(map[uint16]*ChannelInfo),
		topics:   topicMap,
		start:    start,
		end:      end,
	}
}

func (r *Reader) indexedMessageIterator(topics []string, start uint64, end uint64) *indexedMessageIterator {
	topicMap := make(map[string]bool)
	for _, topic := range topics {
		topicMap[topic] = true
	}
	r.l.emitChunks = true
	return &indexedMessageIterator{
		lexer:               r.l,
		rs:                  r.rs,
		channels:            make(map[uint16]*ChannelInfo),
		topics:              topicMap,
		start:               start,
		end:                 end,
		activeChunksetIndex: -1,
		activeChunkIndex:    -1,
	}
}

func (r *Reader) Messages(
	start int64,
	end int64,
	topics []string,
	useIndex bool,
) (MessageIterator, error) {
	if useIndex {
		if rs, ok := r.r.(io.ReadSeeker); ok {
			r.rs = rs
		} else {
			return nil, fmt.Errorf("indexed reader requires a seekable reader")
		}
		return r.indexedMessageIterator(topics, uint64(start), uint64(end)), nil
	}
	return r.unindexedIterator(topics, uint64(start), uint64(end)), nil
}

func (r *Reader) Info() (*Info, error) {
	it := r.indexedMessageIterator(nil, 0, math.MaxUint64)
	err := it.parseIndexSection()
	if err != nil {
		return nil, err
	}
	return &Info{
		Statistics:   it.statistics,
		Channels:     it.channels,
		ChunkIndexes: it.chunkIndexes,
	}, nil
}

func NewReader(r io.Reader) *Reader {
	var rs io.ReadSeeker
	if readseeker, ok := r.(io.ReadSeeker); ok {
		rs = readseeker
	}
	return &Reader{
		l: NewLexer(r, &lexOpts{
			emitChunks: true,
		}),
		r:        r,
		rs:       rs,
		channels: make(map[uint16]*ChannelInfo),
	}
}
