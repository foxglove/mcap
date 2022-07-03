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

// indexedMessageIterator is an iterator over an indexed mcap read seeker (as
// seeking is required). It makes reads in alternation from the index data
// section, the message index at the end of a chunk, and the chunk's contents.
type indexedMessageIterator struct {
	lexer  *Lexer
	rs     io.ReadSeeker
	topics map[string]bool
	start  uint64
	end    uint64

	chunk []byte

	channels          map[uint16]*Channel
	schemas           map[uint16]*Schema
	statistics        *Statistics
	chunkIndexes      []*ChunkIndex
	attachmentIndexes []*AttachmentIndex

	// current location in the index
	activeChunkIndex int // index of the active chunk within the set
	messageOffsets   []MessageIndexEntry
	messageOffsetIdx int
}

// parseIndexSection parses the index section of the file and populates the
// related fields of the structure. It must be called prior to any of the other
// access methods.
func (it *indexedMessageIterator) parseSummarySection() error {
	_, err := it.rs.Seek(-8-4-8-8, io.SeekEnd) // magic, plus 20 bytes footer
	if err != nil {
		return err
	}
	buf := make([]byte, 8+20)
	_, err = io.ReadFull(it.rs, buf)
	if err != nil {
		return fmt.Errorf("read error: %w", err)
	}
	magic := buf[20:]
	if !bytes.Equal(magic, Magic) {
		return fmt.Errorf("not an mcap file")
	}
	footer, err := ParseFooter(buf[:20])
	if err != nil {
		return fmt.Errorf("failed to parse footer: %w", err)
	}

	// scan the whole summary section
	if footer.SummaryStart == 0 {
		return nil
	}
	_, err = it.rs.Seek(int64(footer.SummaryStart), io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to summary start")
	}
	recordBuf := make([]byte, 1024)
	for {
		tokenType, recordReader, recordLen, err := it.lexer.Next()
		if err != nil {
			return fmt.Errorf("failed to get next token: %w", err)
		}
		var record []byte
		if int64(len(recordBuf)) < recordLen {
			recordBuf = make([]byte, recordLen)
			record = recordBuf
		} else {
			record = recordBuf[:recordLen]
		}
		_, err = io.ReadFull(recordReader, record)
		if err != nil {
			return fmt.Errorf("failed to read next record: %w", err)
		}
		switch tokenType {
		case TokenSchema:
			schema, err := ParseSchema(record)
			if err != nil {
				return fmt.Errorf("failed to parse schema: %w", err)
			}
			it.schemas[schema.ID] = schema
		case TokenChannel:
			channelInfo, err := ParseChannel(record)
			if err != nil {
				return fmt.Errorf("failed to parse channel info: %w", err)
			}
			if len(it.topics) == 0 || it.topics[channelInfo.Topic] {
				it.channels[channelInfo.ID] = channelInfo
			}
		case TokenAttachmentIndex:
			idx, err := ParseAttachmentIndex(record)
			if err != nil {
				return fmt.Errorf("failed to parse attachment index: %w", err)
			}
			it.attachmentIndexes = append(it.attachmentIndexes, idx)
		case TokenChunkIndex:
			idx, err := ParseChunkIndex(record)
			if err != nil {
				return fmt.Errorf("failed to parse attachment index: %w", err)
			}
			// if the chunk overlaps with the requested parameters, load it
			for _, channel := range it.channels {
				if idx.MessageIndexOffsets[channel.ID] > 0 {
					if (it.end == 0 && it.start == 0) || (idx.MessageStartTime < it.end && idx.MessageEndTime >= it.start) {
						it.chunkIndexes = append(it.chunkIndexes, idx)
					}
					break
				}
			}
		case TokenStatistics:
			stats, err := ParseStatistics(record)
			if err != nil {
				return fmt.Errorf("failed to parse statistics: %w", err)
			}
			it.statistics = stats
		case TokenFooter:
			return nil
		}
	}
}

func (it *indexedMessageIterator) loadNextChunk() error {
	if it.activeChunkIndex >= len(it.chunkIndexes) {
		return io.EOF
	}
	chunkIndex := it.chunkIndexes[it.activeChunkIndex]
	it.activeChunkIndex++
	_, err := it.rs.Seek(int64(chunkIndex.ChunkStartOffset), io.SeekStart)
	if err != nil {
		return err
	}
	chunk := make([]byte, chunkIndex.ChunkLength+chunkIndex.MessageIndexLength)
	_, err = io.ReadFull(it.rs, chunk)
	if err != nil {
		return fmt.Errorf("failed to read chunk data: %w", err)
	}
	parsedChunk, err := ParseChunk(chunk[9:chunkIndex.ChunkLength])
	if err != nil {
		return fmt.Errorf("failed to parse chunk: %w", err)
	}
	messageIndexSection := chunk[chunkIndex.ChunkLength:]
	var recordLen uint64
	offset := 0
	messageOffsets := []MessageIndexEntry{}
	for offset < len(messageIndexSection) {
		if op := OpCode(messageIndexSection[offset]); op != OpMessageIndex {
			return fmt.Errorf("unexpected token %s in message index section", op)
		}
		offset++
		recordLen, offset, err = getUint64(messageIndexSection, offset)
		if err != nil {
			return fmt.Errorf("failed to get message index record length: %w", err)
		}
		messageIndex, err := ParseMessageIndex(messageIndexSection[offset : uint64(offset)+recordLen])
		if err != nil {
			return fmt.Errorf("failed to parse message index: %w", err)
		}
		offset += int(recordLen)
		// skip message indexes for channels we don't need
		if _, ok := it.channels[messageIndex.ChannelID]; !ok {
			continue
		}
		// append any message index offsets in the requested time range
		for _, offset := range messageIndex.Records {
			if offset.Timestamp >= it.start && offset.Timestamp < it.end {
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
			return fmt.Errorf("failed to read zstd chunk: %w", err)
		}
		defer reader.Close()
		it.chunk, err = io.ReadAll(reader)
		if err != nil {
			return fmt.Errorf("failed to decompress zstd chunk: %w", err)
		}
	case CompressionLZ4:
		reader := lz4.NewReader(bytes.NewReader(parsedChunk.Records))
		it.chunk, err = io.ReadAll(reader)
		if err != nil {
			return fmt.Errorf("failed to decompress lz4 chunk: %w", err)
		}
	default:
		return fmt.Errorf("unsupported compression %s", parsedChunk.Compression)
	}
	return nil
}

func (it *indexedMessageIterator) Next(p []byte) (*Schema, *Channel, *Message, error) {
	if it.statistics == nil {
		err := it.parseSummarySection()
		if err != nil {
			return nil, nil, nil, err
		}
	}
	for it.messageOffsetIdx == len(it.messageOffsets) {
		err := it.loadNextChunk()
		if err != nil {
			return nil, nil, nil, err
		}
	}
	messageOffset := it.messageOffsets[it.messageOffsetIdx]
	it.messageOffsetIdx++
	chunkOffset := messageOffset.Offset
	length := binary.LittleEndian.Uint64(it.chunk[chunkOffset+1:])
	messageData := it.chunk[chunkOffset+1+8 : chunkOffset+1+8+length]
	message, err := ParseMessage(messageData)
	if err != nil {
		return nil, nil, nil, err
	}
	channel := it.channels[message.ChannelID]
	schema := it.schemas[channel.SchemaID]
	return schema, channel, message, nil
}
