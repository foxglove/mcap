package mcap

import (
	"fmt"
	"io"
)

// ParseHeader parses a header record.
func ParseHeader(buf []byte) (*Header, error) {
	profile, offset, err := getPrefixedString(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read profile: %w", err)
	}
	library, _, err := getPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read library: %w", err)
	}
	return &Header{
		Profile: profile,
		Library: library,
	}, nil
}

// ParseFooter parses a footer record.
func ParseFooter(buf []byte) (*Footer, error) {
	summaryStart, offset, err := getUint64(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read summary start: %w", err)
	}
	summaryOffsetStart, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read summary offset start: %w", err)
	}
	summaryCrc, _, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read summary CRC: %w", err)
	}
	return &Footer{
		SummaryStart:       summaryStart,
		SummaryOffsetStart: summaryOffsetStart,
		SummaryCRC:         summaryCrc,
	}, nil
}

// ParseSchema parses a schema record.
func ParseSchema(buf []byte) (*Schema, error) {
	schemaID, offset, err := getUint16(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema ID: %w", err)
	}
	name, offset, err := getPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema name: %w", err)
	}
	encoding, offset, err := getPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read encoding: %w", err)
	}
	data, _, err := getPrefixedBytes(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema data: %w", err)
	}
	return &Schema{
		ID:       schemaID,
		Name:     name,
		Encoding: encoding,
		Data:     append([]byte{}, data...),
	}, nil
}

// ParseChannel parses a channel record.
func ParseChannel(buf []byte) (*Channel, error) {
	channelID, offset, err := getUint16(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read channel id: %w", err)
	}
	schemaID, offset, err := getUint16(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema ID: %w", err)
	}
	topic, offset, err := getPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read topic name: %w", err)
	}
	messageEncoding, offset, err := getPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message encoding: %w", err)
	}
	metadata, _, err := getPrefixedMap(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	return &Channel{
		ID:              channelID,
		SchemaID:        schemaID,
		Topic:           topic,
		MessageEncoding: messageEncoding,
		Metadata:        metadata,
	}, nil
}

// PopulateFrom populates the fields of a Message struct from the message data slice.
func (m *Message) PopulateFrom(buf []byte, copyData bool) error {
	channelID, offset, err := getUint16(buf, 0)
	if err != nil {
		return fmt.Errorf("failed to read channel ID: %w", err)
	}
	sequence, offset, err := getUint32(buf, offset)
	if err != nil {
		return fmt.Errorf("failed to read sequence: %w", err)
	}
	logTime, offset, err := getUint64(buf, offset)
	if err != nil {
		return fmt.Errorf("failed to read record time: %w", err)
	}
	publishTime, offset, err := getUint64(buf, offset)
	if err != nil {
		return fmt.Errorf("failed to read publish time: %w", err)
	}
	data := buf[offset:]
	m.ChannelID = channelID
	m.Sequence = sequence
	m.LogTime = logTime
	m.PublishTime = publishTime
	if copyData {
		m.Data = append(m.Data[:0], data...)
	} else {
		m.Data = data
	}
	return nil
}

// ParseMessage parses a message record.
func ParseMessage(buf []byte) (*Message, error) {
	msg := &Message{}
	if err := msg.PopulateFrom(buf, false); err != nil {
		return nil, err
	}
	return msg, nil
}

// ParseChunk parses a chunk record.
func ParseChunk(buf []byte) (*Chunk, error) {
	messageStartTime, offset, err := getUint64(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read start time: %w", err)
	}
	messageEndTime, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read end time: %w", err)
	}
	uncompressedSize, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read uncompressed size: %w", err)
	}
	uncompressedCRC, offset, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read uncompressed CRC: %w", err)
	}
	compression, offset, err := getPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read compression: %w", err)
	}
	recordsLength, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read compression: %w", err)
	}
	records := buf[offset : offset+int(recordsLength)]
	return &Chunk{
		MessageStartTime: messageStartTime,
		MessageEndTime:   messageEndTime,
		UncompressedSize: uncompressedSize,
		UncompressedCRC:  uncompressedCRC,
		Compression:      compression,
		Records:          records,
	}, nil
}

// ParseMessageIndex parses a message index record.
func ParseMessageIndex(buf []byte) (*MessageIndex, error) {
	channelID, offset, err := getUint16(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read channel ID: %w", err)
	}
	entriesByteLength, offset, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message index entries byte length: %w", err)
	}
	var value, stamp uint64
	var start = offset
	records := make([]MessageIndexEntry, 0, (len(buf)-2)/(8+8))
	for uint32(offset) < uint32(start)+entriesByteLength {
		stamp, offset, err = getUint64(buf, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to read message index entry stamp: %w", err)
		}
		value, offset, err = getUint64(buf, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to read message index entry value: %w", err)
		}
		records = append(records, MessageIndexEntry{
			Timestamp: stamp,
			Offset:    value,
		})
	}
	return &MessageIndex{
		ChannelID:    channelID,
		Records:      records,
		currentIndex: len(records),
	}, nil
}

// ParseChunkIndex parses a chunk index record.
func ParseChunkIndex(buf []byte) (*ChunkIndex, error) {
	messageStartTime, offset, err := getUint64(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read start time: %w", err)
	}
	messageEndTime, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read end time: %w", err)
	}
	chunkStartOffset, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk start offset: %w", err)
	}
	chunkLength, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk length: %w", err)
	}
	msgIndexOffsetsLen, offset, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message index length: %w", err)
	}
	messageIndexOffsets := make(map[uint16]uint64)
	var channelID uint16
	var indexOffset uint64
	inset := 0
	for inset < int(msgIndexOffsetsLen) {
		channelID, inset, err = getUint16(buf[offset:], inset)
		if err != nil {
			return nil, fmt.Errorf("failed to read channel ID: %w", err)
		}
		indexOffset, inset, err = getUint64(buf[offset:], inset)
		if err != nil {
			return nil, fmt.Errorf("failed to read index offset: %w", err)
		}
		messageIndexOffsets[channelID] = indexOffset
	}
	offset += inset
	msgIndexLength, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message index length: %w", err)
	}
	compression, offset, err := getPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read compression: %w", err)
	}
	compressedSize, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read compressed size: %w", err)
	}
	uncompressedSize, _, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read uncompressed size: %w", err)
	}
	return &ChunkIndex{
		MessageStartTime:    messageStartTime,
		MessageEndTime:      messageEndTime,
		ChunkStartOffset:    chunkStartOffset,
		ChunkLength:         chunkLength,
		MessageIndexOffsets: messageIndexOffsets,
		MessageIndexLength:  msgIndexLength,
		Compression:         CompressionFormat(compression),
		CompressedSize:      compressedSize,
		UncompressedSize:    uncompressedSize,
	}, nil
}

func parseAttachmentReader(
	r io.Reader,
	computeCRC bool,
) (*AttachmentReader, error) {
	buf := make([]byte, 8)
	crcReader := newCRCReader(r, computeCRC)
	logTime, err := readUint64(buf, crcReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read record time: %w", err)
	}
	createTime, err := readUint64(buf, crcReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read create time: %w", err)
	}
	name, err := readPrefixedString(buf, crcReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read attachment name: %w", err)
	}
	mediaType, err := readPrefixedString(buf, crcReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read media type: %w", err)
	}
	dataSize, err := readUint64(buf, crcReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read attachment data size: %w", err)
	}
	limitReader := &io.LimitedReader{
		R: crcReader,
		N: int64(dataSize),
	}
	return &AttachmentReader{
		LogTime:    logTime,
		CreateTime: createTime,
		Name:       name,
		MediaType:  mediaType,
		DataSize:   dataSize,

		baseReader: r,
		crcReader:  crcReader,
		data:       limitReader,
	}, nil
}

// ParseAttachmentIndex parses an attachment index record.
func ParseAttachmentIndex(buf []byte) (*AttachmentIndex, error) {
	attachmentOffset, offset, err := getUint64(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read attachment offset: %w", err)
	}
	length, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read attachment length: %w", err)
	}
	logTime, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read record time: %w", err)
	}
	createTime, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read create time: %w", err)
	}
	dataSize, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read data size: %w", err)
	}
	name, offset, err := getPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read attachment name: %w", err)
	}
	mediaType, _, err := getPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read media type: %w", err)
	}
	return &AttachmentIndex{
		Offset:     attachmentOffset,
		Length:     length,
		LogTime:    logTime,
		CreateTime: createTime,
		DataSize:   dataSize,
		Name:       name,
		MediaType:  mediaType,
	}, nil
}

// ParseStatistics parses a statistics record.
func ParseStatistics(buf []byte) (*Statistics, error) {
	if minLength := 8 + 2 + 4 + 4 + 4 + 4 + 4 + 8 + 8; len(buf) < minLength {
		return nil, fmt.Errorf("short statistics record %d < %d: %w", len(buf), minLength, io.ErrShortBuffer)
	}
	messageCount, offset, err := getUint64(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read message count: %w", err)
	}
	schemaCount, offset, err := getUint16(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema count: %w", err)
	}
	channelCount, offset, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read channel count: %w", err)
	}
	attachmentCount, offset, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read attachment count: %w", err)
	}
	metadataCount, offset, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata count: %w", err)
	}
	chunkCount, offset, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk count: %w", err)
	}
	messageStartTime, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message start time: %w", err)
	}
	messageEndTime, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message end time: %w", err)
	}
	channelMessageCountLength, offset, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message count length: %w", err)
	}
	var chanID uint16
	var channelMessageCount uint64
	channelMessageCounts := make(map[uint16]uint64)
	start := offset
	if len(buf) < start+int(channelMessageCountLength) {
		return nil, fmt.Errorf("short channel message count lengths: %w", io.ErrShortBuffer)
	}
	for offset < start+int(channelMessageCountLength) {
		chanID, offset, err = getUint16(buf, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to read message count channel ID: %w", err)
		}
		channelMessageCount, offset, err = getUint64(buf, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to read channel message count: %w", err)
		}
		channelMessageCounts[chanID] = channelMessageCount
	}
	return &Statistics{
		MessageCount:         messageCount,
		SchemaCount:          schemaCount,
		ChannelCount:         channelCount,
		AttachmentCount:      attachmentCount,
		MetadataCount:        metadataCount,
		ChunkCount:           chunkCount,
		MessageStartTime:     messageStartTime,
		MessageEndTime:       messageEndTime,
		ChannelMessageCounts: channelMessageCounts,
	}, nil
}

// ParseMetadata parses a metadata record.
func ParseMetadata(buf []byte) (*Metadata, error) {
	name, offset, err := getPrefixedString(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata name: %w", err)
	}
	metadata, _, err := getPrefixedMap(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	return &Metadata{
		Name:     name,
		Metadata: metadata,
	}, nil
}

// ParseMetadataIndex parses a metadata index record.
func ParseMetadataIndex(buf []byte) (*MetadataIndex, error) {
	recordOffset, offset, err := getUint64(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata offset: %w", err)
	}
	length, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata length: %w", err)
	}
	name, _, err := getPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata name: %w", err)
	}
	return &MetadataIndex{
		Offset: recordOffset,
		Length: length,
		Name:   name,
	}, nil
}

// ParseSummaryOffset parses a summary offset record.
func ParseSummaryOffset(buf []byte) (*SummaryOffset, error) {
	if len(buf) < 17 {
		return nil, io.ErrShortBuffer
	}
	groupOpcode := buf[0]
	offset := 1
	groupStart, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read group start: %w", err)
	}
	groupLength, _, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read group length: %w", err)
	}
	return &SummaryOffset{
		GroupOpcode: OpCode(groupOpcode),
		GroupStart:  groupStart,
		GroupLength: groupLength,
	}, nil
}

// ParseDataEnd parses a data end record.
func ParseDataEnd(buf []byte) (*DataEnd, error) {
	crc, _, err := getUint32(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read CRC: %w", err)
	}
	return &DataEnd{
		DataSectionCRC: crc,
	}, nil
}
