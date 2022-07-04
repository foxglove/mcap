package mcap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// indexedContentIterator is an iterator over an indexed mcap read seeker (as
// seeking is required). It makes reads in alternation from the index data
// section, the message index at the end of a chunk, and the chunk's contents.
type indexedContentIterator struct {
	rs     io.ReadSeeker
	config *contentIteratorConfig
	info   *Info

	chunk []byte

	toplevelRecordIndices      []toplevelRecordIndex
	currentToplevelRecordIndex int
	toplevelRecordBuffer       []byte

	messageOffsets   []MessageIndexEntry
	messageOffsetIdx int
}

func newIndexedContentIterator(rs io.ReadSeeker, info *Info, config *contentIteratorConfig) *indexedContentIterator {
	var toplevelRecordIndices []toplevelRecordIndex
	for _, chunkIndex := range info.ChunkIndexes {
		if config.shouldIncludeChunk(info.Schemas, info.Channels, chunkIndex) {
			toplevelRecordIndices = append(toplevelRecordIndices, chunkIndex)
		}
	}
	for _, attachmentIndex := range info.AttachmentIndexes {
		if config.shouldIncludeAttachment(attachmentIndex) {
			toplevelRecordIndices = append(toplevelRecordIndices, attachmentIndex)
		}
	}
	for _, metadataIndex := range info.MetadataIndexes {
		if config.shouldIncludeMetadata(metadataIndex) {
			toplevelRecordIndices = append(toplevelRecordIndices, metadataIndex)
		}
	}
	sort.Slice(toplevelRecordIndices, func(i, j int) bool {
		return toplevelRecordIndices[i].offset() < toplevelRecordIndices[j].offset()
	})
	return &indexedContentIterator{
		rs:                    rs,
		config:                config,
		info:                  info,
		toplevelRecordIndices: toplevelRecordIndices,
	}
}

func (it *indexedContentIterator) loadNextChunk(chunkIndex *ChunkIndex) (int, error) {
	chunk, err := ReadIntoOrReplace(
		it.rs,
		int64(chunkIndex.ChunkLength+chunkIndex.MessageIndexLength),
		&it.toplevelRecordBuffer,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to read chunk data: %w", err)
	}
	parsedChunk, err := ParseChunk(chunk[9:chunkIndex.ChunkLength])
	if err != nil {
		return 0, fmt.Errorf("failed to parse chunk: %w", err)
	}
	messageIndexSection := chunk[chunkIndex.ChunkLength:]
	var recordLen uint64
	offset := 0
	messageOffsets := []MessageIndexEntry{}
	for offset < len(messageIndexSection) {
		if op := OpCode(messageIndexSection[offset]); op != OpMessageIndex {
			return 0, fmt.Errorf("unexpected token %s in message index section", op)
		}
		offset++
		recordLen, offset, err = getUint64(messageIndexSection, offset)
		if err != nil {
			return 0, fmt.Errorf("failed to get message index record length: %w", err)
		}
		messageIndex, err := ParseMessageIndex(messageIndexSection[offset : uint64(offset)+recordLen])
		if err != nil {
			return 0, fmt.Errorf("failed to parse message index: %w", err)
		}
		offset += int(recordLen)
		// skip message indexes for channels we don't need
		if channel, ok := it.info.Channels[messageIndex.ChannelID]; ok {
			if schema, ok := it.info.Schemas[channel.SchemaID]; ok {
				if it.config.messageFilter == nil || !it.config.messageFilter(schema, channel) {
					continue
				}
			} else {
				continue
			}
		} else {
			continue
		}
		// append any message index offsets in the requested time range
		for _, offset := range messageIndex.Records {
			if it.config.isWithinTimeBounds(offset.Timestamp) {
				messageOffsets = append(messageOffsets, offset)
			}
		}
	}
	sort.Slice(messageOffsets, func(i, j int) bool {
		left := messageOffsets[i]
		right := messageOffsets[j]
		if left.Timestamp < right.Timestamp {
			return true
		}
		if left.Timestamp == right.Timestamp {
			return left.Offset < right.Offset
		}
		return false
	})
	it.messageOffsets = messageOffsets
	it.messageOffsetIdx = 0

	// decompress the chunk data
	switch CompressionFormat(parsedChunk.Compression) {
	case CompressionNone:
		it.chunk = parsedChunk.Records
	case CompressionZSTD:
		reader, err := zstd.NewReader(bytes.NewReader(parsedChunk.Records))
		if err != nil {
			return 0, fmt.Errorf("failed to read zstd chunk: %w", err)
		}
		defer reader.Close()
		it.chunk, err = io.ReadAll(reader)
		if err != nil {
			return 0, fmt.Errorf("failed to decompress zstd chunk: %w", err)
		}
	case CompressionLZ4:
		reader := lz4.NewReader(bytes.NewReader(parsedChunk.Records))
		it.chunk, err = io.ReadAll(reader)
		if err != nil {
			return 0, fmt.Errorf("failed to decompress lz4 chunk: %w", err)
		}
	default:
		return 0, fmt.Errorf("unsupported compression %s", parsedChunk.Compression)
	}
	return len(it.messageOffsets), nil
}

type toplevelRecordIndex interface {
	offset() int64
}

func (ai *AttachmentIndex) offset() int64 { return int64(ai.Offset) }
func (mi *MetadataIndex) offset() int64   { return int64(mi.Offset) }
func (ci *ChunkIndex) offset() int64      { return int64(ci.ChunkStartOffset) }

func (it *indexedContentIterator) nextMessage() (*ResolvedMessage, error) {
	messageOffset := it.messageOffsets[it.messageOffsetIdx]
	it.messageOffsetIdx++
	chunkOffset := messageOffset.Offset
	length := binary.LittleEndian.Uint64(it.chunk[chunkOffset+1:])
	messageData := it.chunk[chunkOffset+1+8 : chunkOffset+1+8+length]
	message, err := ParseMessage(messageData)
	if err != nil {
		return nil, err
	}
	channel := it.info.Channels[message.ChannelID]
	schema := it.info.Schemas[channel.SchemaID]
	return &ResolvedMessage{Message: message, Schema: schema, Channel: channel}, nil
}

func (it *indexedContentIterator) Next(p []byte) (ContentRecord, error) {
	// if we're in a chunk, read off the next message.
	if it.messageOffsetIdx < len(it.messageOffsets) {
		return it.nextMessage()
	}

	// otherwise, jump to the next top-level record.
	for it.currentToplevelRecordIndex < len(it.toplevelRecordIndices) {
		toplevelIndex := it.toplevelRecordIndices[it.currentToplevelRecordIndex]
		_, err := it.rs.Seek(toplevelIndex.offset(), io.SeekStart)
		if err != nil {
			return nil, err
		}
		it.currentToplevelRecordIndex++
		switch v := toplevelIndex.(type) {
		case *MetadataIndex:
			record, err := ReadIntoOrReplace(it.rs, int64(v.Length), &it.toplevelRecordBuffer)
			if err != nil {
				return nil, err
			}
			return ParseMetadata(record)
		case *AttachmentIndex:
			return ParseAttachmentAsReader(it.rs)
		case *ChunkIndex:
			numMessages, err := it.loadNextChunk(v)
			if err != nil {
				return nil, err
			}
			if numMessages == 0 {
				continue
			}
			return it.nextMessage()
		default:
			panic(fmt.Sprintf("unexpected type implementing toplevelRecord: %T", v))
		}
	}
	return nil, io.EOF
}
