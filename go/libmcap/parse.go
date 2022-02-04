package libmcap

import (
	"fmt"
	"io"
)

func ParseHeader(buf []byte) (*Header, error) {
	profile, offset, err := readPrefixedString(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read profile: %w", err)
	}
	library, _, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read library: %w", err)
	}
	return &Header{
		Profile: profile,
		Library: library,
	}, nil
}

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

func ParseChunk(buf []byte) (*Chunk, error) {
	startTime, offset, err := getUint64(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read start time: %w", err)
	}
	endTime, offset, err := getUint64(buf, offset)
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
	compression, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read compression: %w", err)
	}
	records := buf[offset:]
	return &Chunk{
		StartTime:        startTime,
		EndTime:          endTime,
		UncompressedSize: uncompressedSize,
		UncompressedCRC:  uncompressedCRC,
		Compression:      compression,
		Records:          records,
	}, nil
}

func ParseMessageIndex(buf []byte) (*MessageIndex, error) {
	channelID, offset, err := getUint16(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read channel ID: %w", err)
	}
	records, _, err := readMessageIndexEntries(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message index entries: %w", err)
	}
	return &MessageIndex{
		ChannelID: channelID,
		Records:   records,
	}, nil
}

func ParseAttachment(buf []byte) (*Attachment, error) {
	name, offset, err := readPrefixedString(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read attachment name: %w", err)
	}
	createdAt, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read created at: %w", err)
	}
	recordTime, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read record time: %w", err)
	}
	contentType, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read content type: %w", err)
	}
	dataSize, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read attachment data size: %w", err)
	}
	data := buf[offset : offset+int(dataSize)]
	offset += int(dataSize)
	crc, _, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read CRC: %w", err)
	}
	return &Attachment{
		Name:        name,
		CreatedAt:   createdAt,
		RecordTime:  recordTime,
		ContentType: contentType,
		Data:        data,
		CRC:         crc,
	}, nil
}

func ParseAttachmentIndex(buf []byte) (*AttachmentIndex, error) {
	attachmentOffset, offset, err := getUint64(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read attachment offset: %w", err)
	}
	length, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read attachment length: %w", err)
	}
	recordTime, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read record time: %w", err)
	}
	dataSize, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read data size: %w", err)
	}
	name, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read attachment name: %w", err)
	}
	contentType, _, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read content type: %w", err)
	}
	return &AttachmentIndex{
		Offset:      attachmentOffset,
		Length:      length,
		RecordTime:  recordTime,
		DataSize:    dataSize,
		Name:        name,
		ContentType: contentType,
	}, nil
}

func ParseMessage(buf []byte) (*Message, error) {
	channelID, offset, err := getUint16(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read channel ID: %w", err)
	}
	sequence, offset, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read sequence: %w", err)
	}
	publishTime, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read publish time: %w", err)
	}
	recordTime, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read record time: %w", err)
	}
	data := buf[offset:]
	return &Message{
		ChannelID:   channelID,
		Sequence:    sequence,
		RecordTime:  recordTime,
		PublishTime: publishTime,
		Data:        data,
	}, nil
}

func ParseChunkIndex(buf []byte) (*ChunkIndex, error) {
	startTime, offset, err := getUint64(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read start time: %w", err)
	}
	endTime, offset, err := getUint64(buf, offset)
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
	msgIndexLen, offset, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message index length: %w", err)
	}
	messageIndexOffsets := make(map[uint16]uint64)
	var chanID uint16
	var indexOffset uint64
	inset := 0
	for inset < int(msgIndexLen) {
		chanID, inset, err = getUint16(buf[offset:], inset)
		if err != nil {
			return nil, fmt.Errorf("failed to read channel ID: %w", err)
		}
		indexOffset, inset, err = getUint64(buf[offset:], inset)
		if err != nil {
			return nil, fmt.Errorf("failed to read index offset: %w", err)
		}
		messageIndexOffsets[chanID] = indexOffset
	}
	offset += inset
	msgIndexLength, offset, err := getUint64(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message index length: %w", err)
	}
	compression, offset, err := readPrefixedString(buf, offset)
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
		StartTime:           startTime,
		EndTime:             endTime,
		ChunkStartOffset:    chunkStartOffset,
		ChunkLength:         chunkLength,
		MessageIndexOffsets: messageIndexOffsets,
		MessageIndexLength:  msgIndexLength,
		Compression:         CompressionFormat(compression),
		CompressedSize:      compressedSize,
		UncompressedSize:    uncompressedSize,
	}, nil
}

func ParseChannelInfo(buf []byte) (*ChannelInfo, error) {
	channelID, offset, err := getUint16(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read channel id: %w", err)
	}
	topicName, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read topic name: %w", err)
	}
	messageEncoding, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message encoding: %w", err)
	}
	schemaEncoding, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema encoding: %w", err)
	}
	schema, offset, err := readPrefixedBytes(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema: %w", err)
	}
	schemaName, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema name: %w", err)
	}
	metadata, _, err := readPrefixedMap(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	return &ChannelInfo{
		ChannelID:       channelID,
		TopicName:       topicName,
		MessageEncoding: messageEncoding,
		SchemaEncoding:  schemaEncoding,
		SchemaName:      schemaName,
		Schema:          schema,
		Metadata:        metadata,
	}, nil
}

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

func ParseStatistics(buf []byte) (*Statistics, error) {
	if len(buf) < 8+4+4+4+4 {
		return nil, io.ErrShortBuffer
	}
	messageCount, offset, err := getUint64(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read message count: %w", err)
	}
	channelCount, offset, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read channel count: %w", err)
	}
	attachmentCount, offset, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read attachment count: %w", err)
	}
	chunkCount, offset, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk count: %w", err)
	}
	messageCountLen, offset, err := getUint32(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message count length: %w", err)
	}
	var chanID uint16
	var channelMessageCount uint64
	channelMessageCounts := make(map[uint16]uint64)
	start := offset
	if len(buf) < start+int(messageCountLen) {
		return nil, io.ErrShortBuffer
	}
	for offset < start+int(messageCountLen) {
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
		ChannelCount:         channelCount,
		AttachmentCount:      attachmentCount,
		ChunkCount:           chunkCount,
		ChannelMessageCounts: channelMessageCounts,
	}, nil
}

func readMessageIndexEntries(data []byte, offset int) (entries []MessageIndexEntry, newoffset int, err error) {
	entriesByteLength, offset, err := getUint32(data, offset)
	if err != nil {
		return nil, offset, fmt.Errorf("failed to read message index entries byte length: %w", err)
	}
	var value, stamp uint64
	var start = offset
	entries = make([]MessageIndexEntry, 0, (len(data)-2)/(8+8))
	for uint32(offset) < uint32(start)+entriesByteLength {
		stamp, offset, err = getUint64(data, offset)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read message index entry stamp: %w", err)
		}
		value, offset, err = getUint64(data, offset)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read message index entry value: %w", err)
		}
		entries = append(entries, MessageIndexEntry{
			Timestamp: stamp,
			Offset:    value,
		})
	}
	return entries, offset, nil
}
