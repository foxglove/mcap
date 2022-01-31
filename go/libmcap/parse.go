package libmcap

import "io"

func ParseHeader(buf []byte) (*Header, error) {
	profile, offset, err := readPrefixedString(buf, 0)
	if err != nil {
		return nil, err
	}
	library, _, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
	}
	return &Header{
		Profile: profile,
		Library: library,
	}, nil
}

func ParseFooter(buf []byte) (*Footer, error) {
	summaryStart, offset := getUint64(buf, 0)
	summaryOffsetStart, offset := getUint64(buf, offset)
	summaryCrc, _ := getUint32(buf, offset)

	return &Footer{
		SummaryStart:       summaryStart,
		SummaryOffsetStart: summaryOffsetStart,
		SummaryCRC:         summaryCrc,
	}, nil
}

func ParseChunk(buf []byte) (*Chunk, error) {
	startTime, offset := getUint64(buf, 0)
	endTime, offset := getUint64(buf, offset)
	uncompressedSize, offset := getUint64(buf, offset)
	uncompressedCRC, offset := getUint32(buf, offset)
	compression, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
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
	channelID, offset := getUint16(buf, 0)
	records, _ := readMessageIndexEntries(buf, offset)
	return &MessageIndex{
		ChannelID: channelID,
		Records:   records,
	}, nil
}

func ParseAttachment(buf []byte) (*Attachment, error) {
	name, offset, err := readPrefixedString(buf, 0)
	if err != nil {
		return nil, err
	}
	createdAt, offset := getUint64(buf, offset)
	recordTime, offset := getUint64(buf, offset)
	contentType, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
	}
	dataSize, offset := getUint64(buf, offset)
	data := buf[offset : offset+int(dataSize)]
	offset += int(dataSize)
	crc, _ := getUint32(buf, offset)
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
	attachmentOffset, offset := getUint64(buf, 0)
	length, offset := getUint64(buf, offset)
	recordTime, offset := getUint64(buf, offset)
	dataSize, offset := getUint64(buf, offset)
	name, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
	}
	contentType, _, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
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
	if len(buf) < 2+4+8+8 {
		return nil, io.ErrShortBuffer
	}
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
	}, nil
}

func ParseChunkIndex(buf []byte) (*ChunkIndex, error) {
	startTime, offset := getUint64(buf, 0)
	endTime, offset := getUint64(buf, offset)
	chunkStartOffset, offset := getUint64(buf, offset)
	chunkLength, offset := getUint64(buf, offset)
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
	uncompressedSize, _ := getUint64(buf, offset)
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
	channelID, offset := getUint16(buf, 0)
	topicName, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
	}
	messageEncoding, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
	}
	schemaEncoding, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
	}
	schema, offset, err := readPrefixedBytes(buf, offset)
	if err != nil {
		return nil, err
	}
	schemaName, offset, err := readPrefixedString(buf, offset)
	if err != nil {
		return nil, err
	}
	metadata, _, err := readPrefixedMap(buf, offset)
	if err != nil {
		return nil, err
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
	groupStart, offset := getUint64(buf, offset)
	groupLength, _ := getUint64(buf, offset)
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
	messageCount, offset := getUint64(buf, 0)
	channelCount, offset := getUint32(buf, offset)
	attachmentCount, offset := getUint32(buf, offset)
	chunkCount, offset := getUint32(buf, offset)
	messageCountLen, offset := getUint32(buf, offset)
	var chanID uint16
	var channelMessageCount uint64
	channelMessageCounts := make(map[uint16]uint64)
	start := offset
	if len(buf) < start+int(messageCountLen) {
		return nil, io.ErrShortBuffer
	}
	for offset < start+int(messageCountLen) {
		chanID, offset = getUint16(buf, offset)
		channelMessageCount, offset = getUint64(buf, offset)
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

func readMessageIndexEntries(data []byte, offset int) (entries []MessageIndexEntry, newoffset int) {
	entriesByteLength, offset := getUint32(data, offset)
	var value, stamp uint64
	var start = offset
	entries = make([]MessageIndexEntry, 0, (len(data)-2)/(8+8))
	for uint32(offset) < uint32(start)+entriesByteLength {
		stamp, offset = getUint64(data, offset)
		value, offset = getUint64(data, offset)
		entries = append(entries, MessageIndexEntry{
			Timestamp: stamp,
			Offset:    value,
		})
	}
	return entries, offset
}
