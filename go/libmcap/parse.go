package libmcap

import "io"

func ParseChunk(buf []byte) (*Chunk, error) {
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

func ParseMessageIndex(buf []byte) *MessageIndex {
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
	crc, _ := getUint32(buf, offset)
	return &MessageIndex{
		ChannelID: channelID,
		Count:     count,
		Records:   records,
		CRC:       crc,
	}
}

func ParseAttachmentIndex(buf []byte) (*AttachmentIndex, error) {
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
	attachmentOffset, _ := getUint64(buf, offset)
	return &AttachmentIndex{
		RecordTime:     recordTime,
		AttachmentSize: dataSize,
		Name:           name,
		ContentType:    contentType,
		Offset:         attachmentOffset,
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
	uncompressedSize, _ := getUint64(buf, offset)
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

func ParseChannelInfo(buf []byte) (*ChannelInfo, error) {
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
	userdata, _, err := readPrefixedMap(buf, offset)
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

func ParseStatistics(buf []byte) *Statistics {
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
