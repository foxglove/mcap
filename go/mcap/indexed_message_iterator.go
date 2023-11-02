package mcap

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

const (
	chunkBufferGrowthMultiple = 1.2
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
	footer            *Footer

	indexHeap rangeIndexHeap

	zstdDecoder           *zstd.Decoder
	lz4Reader             *lz4.Reader
	hasReadSummarySection bool

	compressedChunkAndMessageIndex []byte
	metadataCallback               func(*Metadata) error
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
	it.footer = footer

	// scan the whole summary section
	if footer.SummaryStart == 0 {
		it.hasReadSummarySection = true
		return nil
	}
	_, err = it.rs.Seek(int64(footer.SummaryStart), io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to summary start")
	}

	lexer, err := NewLexer(bufio.NewReader(it.rs), &LexerOptions{
		SkipMagic: true,
	})
	if err != nil {
		return fmt.Errorf("failed to create lexer: %w", err)
	}
	defer lexer.Close()

	for {
		tokenType, record, err := lexer.Next(nil)
		if err != nil {
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
		case TokenFooter:
			it.hasReadSummarySection = true
			return nil
		}
	}
}

func (it *indexedMessageIterator) loadChunk(chunkIndex *ChunkIndex) error {
	_, err := it.rs.Seek(int64(chunkIndex.ChunkStartOffset), io.SeekStart)
	if err != nil {
		return err
	}

	compressedChunkLength := chunkIndex.ChunkLength + chunkIndex.MessageIndexLength
	if len(it.compressedChunkAndMessageIndex) < int(compressedChunkLength) {
		newSize := int(float64(compressedChunkLength) * chunkBufferGrowthMultiple)
		it.compressedChunkAndMessageIndex = make([]byte, newSize)
	}
	_, err = io.ReadFull(it.rs, it.compressedChunkAndMessageIndex[:compressedChunkLength])
	if err != nil {
		return fmt.Errorf("failed to read chunk data: %w", err)
	}
	parsedChunk, err := ParseChunk(it.compressedChunkAndMessageIndex[9:chunkIndex.ChunkLength])
	if err != nil {
		return fmt.Errorf("failed to parse chunk: %w", err)
	}
	// decompress the chunk data
	var chunkData []byte
	switch CompressionFormat(parsedChunk.Compression) {
	case CompressionNone:
		chunkData = parsedChunk.Records
	case CompressionZSTD:
		if it.zstdDecoder == nil {
			it.zstdDecoder, err = zstd.NewReader(nil)
			if err != nil {
				return fmt.Errorf("failed to instantiate zstd decoder: %w", err)
			}
		}
		chunkData = make([]byte, 0, parsedChunk.UncompressedSize)
		chunkData, err = it.zstdDecoder.DecodeAll(parsedChunk.Records, chunkData)
		if err != nil {
			return fmt.Errorf("failed to decode chunk data: %w", err)
		}
	case CompressionLZ4:
		if it.lz4Reader == nil {
			it.lz4Reader = lz4.NewReader(bytes.NewReader(parsedChunk.Records))
		} else {
			it.lz4Reader.Reset(bytes.NewReader(parsedChunk.Records))
		}
		chunkData = make([]byte, parsedChunk.UncompressedSize)
		_, err = io.ReadFull(it.lz4Reader, chunkData)
		if err != nil {
			return fmt.Errorf("failed to decompress lz4 chunk: %w", err)
		}
	default:
		return fmt.Errorf("unsupported compression %s", parsedChunk.Compression)
	}
	// use the message index to find the messages we want from the chunk
	messageIndexSection := it.compressedChunkAndMessageIndex[chunkIndex.ChunkLength:compressedChunkLength]
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

func readRecord(r io.Reader) (TokenType, []byte, error) {
	buf := make([]byte, 9)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read record header: %w", err)
	}
	tokenType := TokenType(buf[0])
	recordLen := binary.LittleEndian.Uint64(buf[1:])
	record := make([]byte, recordLen)
	_, err = io.ReadFull(r, record)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read record: %w", err)
	}
	return tokenType, record, nil
}

func (it *indexedMessageIterator) Next(_ []byte) (*Schema, *Channel, *Message, error) {
	if !it.hasReadSummarySection {
		err := it.parseSummarySection()
		if err != nil {
			return nil, nil, nil, err
		}
		// take care of the metadata here
		if it.metadataCallback != nil {
			for _, idx := range it.metadataIndexes {
				_, err = it.rs.Seek(int64(idx.Offset), io.SeekStart)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("failed to seek to metadata: %w", err)
				}
				tokenType, data, err := readRecord(it.rs)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("failed to read metadata record: %w", err)
				}
				if tokenType != TokenMetadata {
					return nil, nil, nil, fmt.Errorf("expected metadata record, found %v", data)
				}
				metadata, err := ParseMetadata(data)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("failed to parse metadata record: %w", err)
				}
				err = it.metadataCallback(metadata)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("metadata callback failed: %w", err)
				}
			}
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
