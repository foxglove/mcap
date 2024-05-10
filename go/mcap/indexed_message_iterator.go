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

// indexedMessageIterator is an iterator over an indexed mcap io.ReadSeeker (as
// seeking is required). It reads index information from the MCAP summary section first, then
// seeks to chunk records in the data section.
//
// This iterator yields messages using one of two strategies depending on whether any chunks have
// overlapping time ranges.
//   - If no chunks overlap, it seeks to each chunk in sequence, decompresses it and builds an
//     in-memory ordered array of message indexes. Then it iterates through this array, copying
//     all messages out of the decompressed chunk before seeking to the next chunk.
//   - If some chunks have overlapping time ranges, it uses a heap containing both message and
//     chunk indexes. Items are popped from the heap in time order. When a chunk index is popped
//     from the heap, the chunk is decompressed and its message indexes are pushed into the heap.
//     When a message index is popped from the heap, the corresponding message is copied out of the
//     decompressed chunk and yielded.
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

	useHeap            bool
	indexHeap          rangeIndexHeap
	curChunkIndex      int
	messageIndexes     []MessageIndexEntry
	curMessageIndex    int
	decompressedChunks []decompressedChunk

	zstdDecoder *zstd.Decoder
	lz4Reader   *lz4.Reader
	initialized bool

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
		it.initialized = true
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
			return nil
		}
	}
}

// initializeReader uses summary information to determine the order chunks will be read and
// what technique will be used to read them.
func (it *indexedMessageIterator) initializeReader() error {
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
		// check if chunks overlap, if so, we need to use a heap when loading messages.
		for i := 1; i < len(it.chunkIndexes); i++ {
			prev := it.chunkIndexes[i-1]
			cur := it.chunkIndexes[i]
			if prev.MessageEndTime > cur.MessageStartTime {
				it.useHeap = true
				break
			}
		}
	case ReverseLogTimeOrder:
		sort.Slice(it.chunkIndexes, func(i, j int) bool {
			return it.chunkIndexes[i].MessageEndTime > it.chunkIndexes[j].MessageEndTime
		})
		// check if chunks overlap, if so, we need to use a heap when loading messages.
		for i := 1; i < len(it.chunkIndexes); i++ {
			prev := it.chunkIndexes[i-1]
			cur := it.chunkIndexes[i]
			if prev.MessageStartTime < cur.MessageEndTime {
				it.useHeap = true
				break
			}
		}
	}
	// if the heap is needed, initialize it with the full set of chunk indexes.
	if it.useHeap {
		if it.order == ReverseLogTimeOrder {
			it.indexHeap.reverse = true
		}
		for _, idx := range it.chunkIndexes {
			if err := it.indexHeap.PushChunkIndex(idx); err != nil {
				return err
			}
		}
	}
	it.initialized = true
	return nil
}

// loadChunk seeks to and decompresses a chunk into a chunk slot, then populates it.messageIndexes
// with the offsets of messages in that chunk. If it.useHead is true, it pushes all message indexes
// into the heap.
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
	chunkIsOrdered := true
	var maxLogTime uint64
	it.messageIndexes = it.messageIndexes[:0]
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
					it.messageIndexes = append(it.messageIndexes, MessageIndexEntry{Timestamp: msg.LogTime, Offset: offset})
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
	// If using the heap, push all message indexes onto the heap.
	if it.useHeap {
		for _, mi := range it.messageIndexes {
			if err := it.indexHeap.PushMessage(
				chunkIndex,
				chunkSlotIndex,
				mi.Timestamp,
				mi.Offset,
			); err != nil {
				return err
			}
		}
		return nil
	}
	// otherwise, we'll iterate directly on the array of message indexes. Sort them into
	// the required order.
	switch it.order {
	case FileOrder:
		// message indexes are already in file order, no sorting needed
	case LogTimeOrder:
		if !chunkIsOrdered {
			// need to stable-sort to ensure messages with equivalent timestamps remain in
			// the same order
			sort.SliceStable(it.messageIndexes, func(i, j int) bool {
				return it.messageIndexes[i].Timestamp < it.messageIndexes[j].Timestamp
			})
		}
	case ReverseLogTimeOrder:
		if !chunkIsOrdered {
			sort.SliceStable(it.messageIndexes, func(i, j int) bool {
				return it.messageIndexes[i].Timestamp < it.messageIndexes[j].Timestamp
			})
		}
		slices.Reverse(it.messageIndexes)
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
	if !it.initialized {
		if err := it.parseSummarySection(); err != nil {
			return nil, nil, nil, err
		}
		if err := it.initializeReader(); err != nil {
			return nil, nil, nil, err
		}
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

	if !it.useHeap {
		if it.curMessageIndex >= len(it.messageIndexes) {
			if it.curChunkIndex >= len(it.chunkIndexes) {
				return nil, nil, nil, io.EOF
			}
			chunkIndex := it.chunkIndexes[it.curChunkIndex]
			if err := it.loadChunk(chunkIndex); err != nil {
				return nil, nil, nil, err
			}
			it.curMessageIndex = 0
			it.curChunkIndex++
			return it.Next2(msg)
		}
		messageIndex := it.messageIndexes[it.curMessageIndex]
		decompressedChunk := &it.decompressedChunks[0]
		if err := loadMessageAtOffset(decompressedChunk.buf, messageIndex.Offset, msg); err != nil {
			return nil, nil, nil, err
		}
		decompressedChunk.unreadMessages--
		it.curMessageIndex++
		schema, channel, err := it.getSchemaAndChannel(msg.ChannelID)
		return schema, channel, msg, err
	}

	if it.indexHeap.len() == 0 {
		return nil, nil, nil, io.EOF
	}
	ri, err := it.indexHeap.Pop()
	if err != nil {
		return nil, nil, nil, err
	}
	if ri.ChunkSlotIndex == -1 {
		if err := it.loadChunk(ri.chunkIndex); err != nil {
			return nil, nil, nil, err
		}
		return it.Next2(msg)
	}
	decompressedChunk := &it.decompressedChunks[ri.ChunkSlotIndex]
	if err := loadMessageAtOffset(decompressedChunk.buf, ri.MessageOffsetInChunk, msg); err != nil {
		return nil, nil, nil, err
	}
	decompressedChunk.unreadMessages--
	schema, channel, err := it.getSchemaAndChannel(msg.ChannelID)
	return schema, channel, msg, err
}

func (it *indexedMessageIterator) getSchemaAndChannel(channelID uint16) (*Schema, *Channel, error) {
	channel := slicemap.GetAt(it.channels, channelID)
	if channel == nil {
		return nil, nil, fmt.Errorf("message with unrecognized channel ID %d", channelID)
	}
	schema := slicemap.GetAt(it.schemas, channel.SchemaID)
	if schema == nil && channel.SchemaID != 0 {
		return nil, nil, fmt.Errorf("channel %d with unrecognized schema ID %d", channelID, channel.SchemaID)
	}
	return schema, channel, nil
}

// loadMessageAtOffset loads the Message record from `decompressedChunk` into `msg`.
func loadMessageAtOffset(decompressedChunk []byte, offset uint64, msg *Message) error {
	length := binary.LittleEndian.Uint64(decompressedChunk[offset+1:])
	messageData := decompressedChunk[offset+1+8 : offset+1+8+length]
	if err := msg.PopulateFrom(messageData, true); err != nil {
		return err
	}
	return nil
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
