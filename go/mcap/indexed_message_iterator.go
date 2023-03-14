package mcap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

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

	channels          map[uint16]*Channel
	schemas           map[uint16]*Schema
	statistics        *Statistics
	chunkIndexes      []*ChunkIndex
	attachmentIndexes []*AttachmentIndex
	metadataIndexes   []*MetadataIndex

	indexHeap rangeIndexHeap

	zstdDecoder           *zstd.Decoder
	lz4Reader             *lz4.Reader
	hasReadSummarySection bool
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
		return fmt.Errorf("not an MCAP file")
	}
	footer, err := ParseFooter(buf[:20])
	if err != nil {
		return fmt.Errorf("failed to parse footer: %w", err)
	}

	// scan the whole summary section
	if footer.SummaryStart == 0 {
		it.hasReadSummarySection = true
		return nil
	}
	_, err = it.rs.Seek(int64(footer.SummaryStart), io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to summary start")
	}

	summarySection := make([]byte, footer.SummaryOffsetStart-footer.SummaryStart)
	_, err = io.ReadFull(it.rs, summarySection)
	if err != nil {
		return fmt.Errorf("failed to read summary section")
	}

	lexer, err := NewLexer(bytes.NewReader(summarySection), &LexerOptions{
		SkipMagic: true,
	})
	if err != nil {
		return fmt.Errorf("failed to create lexer: %w", err)
	}
	defer lexer.Close()

	for {
		tokenType, record, err := lexer.Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				it.hasReadSummarySection = true
				return nil
			}
			return fmt.Errorf("failed to get next token: %w", err)
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
		case TokenMetadataIndex:
			idx, err := ParseMetadataIndex(record)
			if err != nil {
				return fmt.Errorf("failed to parse metadata index: %w", err)
			}
			it.metadataIndexes = append(it.metadataIndexes, idx)
		case TokenChunkIndex:
			idx, err := ParseChunkIndex(record)
			if err != nil {
				return fmt.Errorf("failed to parse attachment index: %w", err)
			}
			it.chunkIndexes = append(it.chunkIndexes, idx)
			// if the chunk overlaps with the requested parameters, load it
			for _, channel := range it.channels {
				if idx.MessageIndexOffsets[channel.ID] > 0 {
					if (it.end == 0 && it.start == 0) || (idx.MessageStartTime < it.end && idx.MessageEndTime >= it.start) {
						rangeIndex := rangeIndex{
							chunkIndex: idx,
						}
						if err := it.indexHeap.HeapPush(rangeIndex); err != nil {
							return err
						}
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
		}
	}
}

func (it *indexedMessageIterator) loadChunk(chunkIndex *ChunkIndex) error {
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
	// decompress the chunk data
	var chunkData []byte
	switch CompressionFormat(parsedChunk.Compression) {
	case CompressionNone:
		chunkData = parsedChunk.Records
	case CompressionZSTD:
		var err error
		if it.zstdDecoder == nil {
			it.zstdDecoder, err = zstd.NewReader(bytes.NewReader(parsedChunk.Records))
		} else {
			err = it.zstdDecoder.Reset(bytes.NewReader(parsedChunk.Records))
		}
		if err != nil {
			return fmt.Errorf("failed to read zstd chunk: %w", err)
		}
		chunkData, err = io.ReadAll(it.zstdDecoder)
		if err != nil {
			return fmt.Errorf("failed to decompress zstd chunk: %w", err)
		}
	case CompressionLZ4:
		if it.lz4Reader == nil {
			it.lz4Reader = lz4.NewReader(bytes.NewReader(parsedChunk.Records))
		} else {
			it.lz4Reader.Reset(bytes.NewReader(parsedChunk.Records))
		}
		chunkData, err = io.ReadAll(it.lz4Reader)
		if err != nil {
			return fmt.Errorf("failed to decompress lz4 chunk: %w", err)
		}
	default:
		return fmt.Errorf("unsupported compression %s", parsedChunk.Compression)
	}
	// use the message index to find the messages we want from the chunk
	messageIndexSection := chunk[chunkIndex.ChunkLength:]
	var recordLen uint64
	offset := 0
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
		// push any message index entries in the requested time range to the heap to read.
		for i := range messageIndex.Records {
			timestamp := messageIndex.Records[i].Timestamp
			if timestamp >= it.start && timestamp < it.end {
				if err := it.indexHeap.HeapPush(rangeIndex{
					chunkIndex:        chunkIndex,
					messageIndexEntry: &messageIndex.Records[i],
					buf:               chunkData,
				}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (it *indexedMessageIterator) Next(p []byte) (*Schema, *Channel, *Message, error) {
	if !it.hasReadSummarySection {
		err := it.parseSummarySection()
		if err != nil {
			return nil, nil, nil, err
		}
	}
	for it.indexHeap.Len() > 0 {
		ri, err := it.indexHeap.HeapPop()
		if err != nil {
			return nil, nil, nil, err
		}
		if ri.messageIndexEntry == nil {
			err := it.loadChunk(ri.chunkIndex)
			if err != nil {
				return nil, nil, nil, err
			}
			continue
		}
		chunkOffset := ri.messageIndexEntry.Offset
		length := binary.LittleEndian.Uint64(ri.buf[chunkOffset+1:])
		messageData := ri.buf[chunkOffset+1+8 : chunkOffset+1+8+length]
		message, err := ParseMessage(messageData)
		if err != nil {
			return nil, nil, nil, err
		}
		channel := it.channels[message.ChannelID]
		schema := it.schemas[channel.SchemaID]
		return schema, channel, message, nil
	}
	return nil, nil, nil, io.EOF
}
