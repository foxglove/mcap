package mcap

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"slices"
	"sort"

	"github.com/foxglove/mcap/go/mcap/slicemap"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

const (
	chunkBufferGrowthMultiple = 1.2
)

type decompressedChunk struct {
	buf            []byte
	unreadMessages uint64
}

type messageIndexWithChunkSlot struct {
	timestamp      uint64
	offset         uint64
	chunkSlotIndex int
}

// indexedMessageIterator is an iterator over an indexed mcap io.ReadSeeker (as
// seeking is required). It reads index information from the MCAP summary section first, then
// seeks to chunk records in the data section.
//
// This iterator seeks to chunks in the MCAP, decompresses them, builds an in-memory message index,
// and yields messages according to that index.
type indexedMessageIterator struct {
	lexer  *Lexer
	rs     io.ReadSeeker
	topics map[string]bool
	start  uint64
	end    uint64
	order  ReadOrder

	channels          []*Channel
	schemas           []*Schema
	statistics        *Statistics
	chunkIndexes      []*ChunkIndex
	attachmentIndexes []*AttachmentIndex
	metadataIndexes   []*MetadataIndex
	footer            *Footer

	curChunkIndex      int
	messageIndexes     []messageIndexWithChunkSlot
	curMessageIndex    int
	decompressedChunks []decompressedChunk

	zstdDecoder           *zstd.Decoder
	lz4Reader             *lz4.Reader
	hasReadSummarySection bool

	recordBuf        []byte
	metadataCallback func(*Metadata) error
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
			it.schemas = slicemap.SetAt(it.schemas, schema.ID, schema)
		case TokenChannel:
			channelInfo, err := ParseChannel(record)
			if err != nil {
				return fmt.Errorf("failed to parse channel info: %w", err)
			}
			if len(it.topics) == 0 || it.topics[channelInfo.Topic] {
				it.channels = slicemap.SetAt(it.channels, channelInfo.ID, channelInfo)
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
			// if the chunk overlaps with the requested parameters, load it
			for _, channel := range it.channels {
				if channel != nil && idx.MessageIndexOffsets[channel.ID] > 0 {
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
			// sort chunk indexes in the order that they will need to be loaded, depending on the specified
			// read order.
			switch it.order {
			case FileOrder:
				sort.Slice(it.chunkIndexes, func(i, j int) bool {
					return it.chunkIndexes[i].ChunkStartOffset < it.chunkIndexes[j].ChunkStartOffset
				})
			case LogTimeOrder:
				sort.Slice(it.chunkIndexes, func(i, j int) bool {
					return it.chunkIndexes[i].MessageStartTime < it.chunkIndexes[j].MessageStartTime
				})
			case ReverseLogTimeOrder:
				sort.Slice(it.chunkIndexes, func(i, j int) bool {
					return it.chunkIndexes[i].MessageEndTime > it.chunkIndexes[j].MessageEndTime
				})
			}
			return nil
		}
	}
}

// loadChunk seeks to and decompresses a chunk into a chunk slot, then populates it.messageIndexes
// with the offsets of messages in that chunk.
func (it *indexedMessageIterator) loadChunk(chunkIndex *ChunkIndex) error {
	_, err := it.rs.Seek(int64(chunkIndex.ChunkStartOffset), io.SeekStart)
	if err != nil {
		return err
	}

	compressedChunkLength := chunkIndex.ChunkLength
	if len(it.recordBuf) < int(compressedChunkLength) {
		newSize := int(float64(compressedChunkLength) * chunkBufferGrowthMultiple)
		it.recordBuf = make([]byte, newSize)
	}
	_, err = io.ReadFull(it.rs, it.recordBuf[:compressedChunkLength])
	if err != nil {
		return fmt.Errorf("failed to read chunk data: %w", err)
	}
	parsedChunk, err := ParseChunk(it.recordBuf[9:chunkIndex.ChunkLength])
	if err != nil {
		return fmt.Errorf("failed to parse chunk: %w", err)
	}
	// decompress the chunk data
	chunkSlotIndex := -1
	for i, decompressedChunk := range it.decompressedChunks {
		if decompressedChunk.unreadMessages == 0 {
			chunkSlotIndex = i
			break
		}
	}
	if chunkSlotIndex == -1 {
		it.decompressedChunks = append(it.decompressedChunks, decompressedChunk{})
		chunkSlotIndex = len(it.decompressedChunks) - 1
	}
	chunkSlot := &it.decompressedChunks[chunkSlotIndex]
	bufSize := parsedChunk.UncompressedSize
	if uint64(cap(chunkSlot.buf)) < bufSize {
		chunkSlot.buf = make([]byte, bufSize)
	} else {
		chunkSlot.buf = chunkSlot.buf[:bufSize]
	}
	switch CompressionFormat(parsedChunk.Compression) {
	case CompressionNone:
		copy(chunkSlot.buf, parsedChunk.Records)
	case CompressionZSTD:
		if it.zstdDecoder == nil {
			it.zstdDecoder, err = zstd.NewReader(nil)
			if err != nil {
				return fmt.Errorf("failed to instantiate zstd decoder: %w", err)
			}
		}
		chunkSlot.buf, err = it.zstdDecoder.DecodeAll(parsedChunk.Records, chunkSlot.buf[:0])
		if err != nil {
			return fmt.Errorf("failed to decode chunk data: %w", err)
		}
	case CompressionLZ4:
		if it.lz4Reader == nil {
			it.lz4Reader = lz4.NewReader(bytes.NewReader(parsedChunk.Records))
		} else {
			it.lz4Reader.Reset(bytes.NewReader(parsedChunk.Records))
		}
		_, err = io.ReadFull(it.lz4Reader, chunkSlot.buf)
		if err != nil {
			return fmt.Errorf("failed to decompress lz4 chunk: %w", err)
		}
	default:
		return fmt.Errorf("unsupported compression %s", parsedChunk.Compression)
	}
	// produce message indexes for the newly decompressed chunk data.
	var maxLogTime uint64
	// if there are no message indexes outstanding, truncate now.
	if it.curMessageIndex == len(it.messageIndexes) {
		it.curMessageIndex = 0
		it.messageIndexes = it.messageIndexes[:0]
	}
	chunkIsOrdered := it.curMessageIndex == 0
	startIdx := len(it.messageIndexes)
	for offset := uint64(0); offset < bufSize; {
		if bufSize < offset+1+8 {
			return fmt.Errorf("expected another record in chunk, but left with %d bytes", bufSize-offset)
		}
		opcodeAndLengthBuf := chunkSlot.buf[offset : offset+1+8]
		op := OpCode(opcodeAndLengthBuf[0])
		recordLen := binary.LittleEndian.Uint64(opcodeAndLengthBuf[1:])
		recordStart := offset + 1 + 8
		recordEnd, overflow := checkedAdd(recordStart, recordLen)
		if overflow {
			return fmt.Errorf("record length extends past uint64 range: start: %d, len: %d", recordStart, recordLen)
		}
		if bufSize < recordEnd {
			return fmt.Errorf(
				"%s record in chunk has length %d bytes but only %d remaining in chunk",
				op, recordLen, bufSize-recordStart)
		}
		recordContent := chunkSlot.buf[recordStart:recordEnd]
		msg := Message{}
		if op == OpMessage {
			if err := msg.PopulateFrom(recordContent, false); err != nil {
				return fmt.Errorf("could not parse message in chunk: %w", err)
			}
			if slicemap.GetAt(it.channels, msg.ChannelID) != nil {
				if msg.LogTime >= it.start && msg.LogTime < it.end {
					it.messageIndexes = append(it.messageIndexes, messageIndexWithChunkSlot{
						timestamp:      msg.LogTime,
						offset:         offset,
						chunkSlotIndex: chunkSlotIndex,
					})
					if msg.LogTime < maxLogTime {
						chunkIsOrdered = false
					} else {
						maxLogTime = msg.LogTime
					}
					chunkSlot.unreadMessages++
				}
			}
		}
		offset = recordEnd
	}
	unreadMessageIndexes := it.messageIndexes[it.curMessageIndex:]
	switch it.order {
	case FileOrder:
		// message indexes are already in file order, no sorting needed
	case LogTimeOrder:
		if !chunkIsOrdered {
			sort.Slice(unreadMessageIndexes, func(i, j int) bool {
				if unreadMessageIndexes[i].timestamp == unreadMessageIndexes[j].timestamp {
					return unreadMessageIndexes[i].offset < unreadMessageIndexes[j].offset
				}
				return unreadMessageIndexes[i].timestamp < unreadMessageIndexes[j].timestamp
			})
		}
	case ReverseLogTimeOrder:
		// assume message indexes will always be mostly-in-order, so reversing the newly-added
		// indexes will put them mostly into reverse order. If the chunk is in order,
		// that's all we need to do.
		slices.Reverse(it.messageIndexes[startIdx:])
		if !chunkIsOrdered {
			sort.Slice(unreadMessageIndexes, func(i, j int) bool {
				if unreadMessageIndexes[i].timestamp == unreadMessageIndexes[j].timestamp {
					return unreadMessageIndexes[i].offset > unreadMessageIndexes[j].offset
				}
				return unreadMessageIndexes[i].timestamp > unreadMessageIndexes[j].timestamp
			})
		}
	}
	// if there is more dead space at the front than unread, remove the dead space by
	// copying the live data to the front and truncating.
	if len(unreadMessageIndexes) < it.curMessageIndex {
		copy(it.messageIndexes[:it.curMessageIndex], unreadMessageIndexes)
		it.messageIndexes = it.messageIndexes[:len(unreadMessageIndexes)]
		it.curMessageIndex = 0
	}

	return nil
}

func readRecord(r io.Reader) (OpCode, []byte, error) {
	buf := make([]byte, 9)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read record header: %w", err)
	}
	opcode := OpCode(buf[0])
	recordLen := binary.LittleEndian.Uint64(buf[1:])
	record := make([]byte, recordLen)
	_, err = io.ReadFull(r, record)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read record: %w", err)
	}
	return opcode, record, nil
}

// Next2 yields the next message from the iterator, writing the result into the provided Message
// struct. The msg.Data buffer will be reused if it has enough capacity. If `msg` is nil, a new
// Message will be allocated.
func (it *indexedMessageIterator) Next2(msg *Message) (*Schema, *Channel, *Message, error) {
	if msg == nil {
		msg = &Message{}
	}
	if !it.hasReadSummarySection {
		if err := it.parseSummarySection(); err != nil {
			return nil, nil, nil, err
		}
		it.hasReadSummarySection = true
		// take care of the metadata here
		if it.metadataCallback != nil {
			for _, idx := range it.metadataIndexes {
				_, err := it.rs.Seek(int64(idx.Offset), io.SeekStart)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("failed to seek to metadata: %w", err)
				}
				opcode, data, err := readRecord(it.rs)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("failed to read metadata record: %w", err)
				}
				if opcode != OpMetadata {
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

	// if there are no indexed messages to yield, load another chunk
	if it.curMessageIndex >= len(it.messageIndexes) {
		// if there are no more chunks, iteration ends
		if it.curChunkIndex >= len(it.chunkIndexes) {
			return nil, nil, nil, io.EOF
		}
		chunkIndex := it.chunkIndexes[it.curChunkIndex]
		if err := it.loadChunk(chunkIndex); err != nil {
			return nil, nil, nil, err
		}
		it.curChunkIndex++
		return it.Next2(msg)
	}
	// if there are more chunks left, check if the next one should be loaded before yielding another
	// message
	if it.curChunkIndex < len(it.chunkIndexes) {
		chunkIndex := it.chunkIndexes[it.curChunkIndex]
		messageIndex := it.messageIndexes[it.curMessageIndex]
		if (it.order == LogTimeOrder && chunkIndex.MessageStartTime < messageIndex.timestamp) ||
			(it.order == ReverseLogTimeOrder && chunkIndex.MessageEndTime > messageIndex.timestamp) {
			if err := it.loadChunk(chunkIndex); err != nil {
				return nil, nil, nil, err
			}
			it.curChunkIndex++
			return it.Next2(msg)
		}
	}
	// yield the next message
	messageIndex := it.messageIndexes[it.curMessageIndex]
	decompressedChunk := &it.decompressedChunks[messageIndex.chunkSlotIndex]
	length := binary.LittleEndian.Uint64(decompressedChunk.buf[messageIndex.offset+1:])
	messageData := decompressedChunk.buf[messageIndex.offset+1+8 : messageIndex.offset+1+8+length]
	if err := msg.PopulateFrom(messageData, true); err != nil {
		return nil, nil, nil, err
	}
	decompressedChunk.unreadMessages--
	it.curMessageIndex++
	channel := slicemap.GetAt(it.channels, msg.ChannelID)
	if channel == nil {
		return nil, nil, nil, fmt.Errorf("message with unrecognized channel ID %d", msg.ChannelID)
	}
	schema := slicemap.GetAt(it.schemas, channel.SchemaID)
	if schema == nil && channel.SchemaID != 0 {
		return nil, nil, nil, fmt.Errorf("channel %d with unrecognized schema ID %d", msg.ChannelID, channel.SchemaID)
	}
	return schema, channel, msg, nil
}

func (it *indexedMessageIterator) Next(buf []byte) (*Schema, *Channel, *Message, error) {
	msg := &Message{Data: buf}
	return it.Next2(msg)
}

// returns the sum of two uint64s, with a boolean indicating if the sum overflowed.
func checkedAdd(a, b uint64) (uint64, bool) {
	res, carry := bits.Add64(a, b, 0)
	return res, carry != 0
}
