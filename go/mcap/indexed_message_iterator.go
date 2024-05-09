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

	chunksOverlap bool
	indexHeap     rangeIndexHeap

	chunkIndexesToLoad []*ChunkIndex
	curChunkIndex      int
	messageIndexes     []MessageIndexEntry
	curMessageIndex    int

	zstdDecoder           *zstd.Decoder
	lz4Reader             *lz4.Reader
	hasReadSummarySection bool

	recordBuf          []byte
	decompressedChunks []decompressedChunk
	metadataCallback   func(*Metadata) error
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

lexerloop:
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
						it.chunkIndexesToLoad = append(it.chunkIndexesToLoad, idx)
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
			break lexerloop
		}
	}
	it.chunksOverlap = false
	switch it.indexHeap.order {
	case FileOrder:
		sort.Slice(it.chunkIndexesToLoad, func(i, j int) bool {
			return it.chunkIndexesToLoad[i].ChunkStartOffset < it.chunkIndexesToLoad[j].ChunkStartOffset
		})
	case LogTimeOrder:
		sort.Slice(it.chunkIndexesToLoad, func(i, j int) bool {
			return it.chunkIndexesToLoad[i].MessageStartTime < it.chunkIndexesToLoad[j].MessageStartTime
		})
		for i := 1; i < len(it.chunkIndexesToLoad); i++ {
			prev := it.chunkIndexesToLoad[i-1]
			cur := it.chunkIndexesToLoad[i]
			if prev.MessageEndTime > cur.MessageStartTime {
				it.chunksOverlap = true
				break
			}
		}
	case ReverseLogTimeOrder:
		sort.Slice(it.chunkIndexesToLoad, func(i, j int) bool {
			return it.chunkIndexesToLoad[i].MessageEndTime > it.chunkIndexesToLoad[j].MessageEndTime
		})
		for i := 1; i < len(it.chunkIndexesToLoad); i++ {
			prev := it.chunkIndexesToLoad[i-1]
			cur := it.chunkIndexesToLoad[i]
			if prev.MessageStartTime < cur.MessageEndTime {
				it.chunksOverlap = true
				break
			}
		}
	}
	if it.chunksOverlap {
		for _, idx := range it.chunkIndexesToLoad {
			if err := it.indexHeap.PushChunkIndex(idx); err != nil {
				return err
			}
		}
	}
	return nil
}

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
	chunkIsOrdered := true
	var maxLogTime uint64
	it.messageIndexes = it.messageIndexes[:0]
	// lex the chunk
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
				op.String(), recordLen, bufSize-recordStart)
		}
		recordContent := chunkSlot.buf[recordStart:recordEnd]
		msg := Message{}
		switch op {
		case OpMessage:
			if err := msg.PopulateFrom(recordContent); err != nil {
				return fmt.Errorf("could not parse message in chunk: %w", err)
			}
			if _, ok := it.channels[msg.ChannelID]; ok {
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
		case OpChannel, OpSchema:
			// no-op
		default:
			return fmt.Errorf(
				"expected only schema, channel, message opcodes in chunk, found %s at offset %d",
				op.String(),
				offset,
			)
		}
		offset = recordEnd
	}
	if it.chunksOverlap {
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
	if !chunkIsOrdered && it.indexHeap.order != FileOrder {
		sort.Slice(it.messageIndexes, func(i, j int) bool {
			return it.messageIndexes[i].Timestamp < it.messageIndexes[j].Timestamp
		})
	}
	if it.indexHeap.order == ReverseLogTimeOrder {
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

func (it *indexedMessageIterator) NextInto(msg *Message) (*Schema, *Channel, *Message, error) {
	if msg == nil {
		msg = &Message{}
	}
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

	if !it.chunksOverlap {
		if it.curMessageIndex >= len(it.messageIndexes) {
			if it.curChunkIndex >= len(it.chunkIndexesToLoad) {
				return nil, nil, nil, io.EOF
			}
			chunkIndex := it.chunkIndexesToLoad[it.curChunkIndex]
			if err := it.loadChunk(chunkIndex); err != nil {
				return nil, nil, nil, err
			}
			it.curMessageIndex = 0
			it.curChunkIndex++
			return it.NextInto(msg)
		}
		chunkOffset := it.messageIndexes[it.curMessageIndex].Offset
		decompressedChunk := it.decompressedChunks[0]
		length := binary.LittleEndian.Uint64(decompressedChunk.buf[chunkOffset+1:])
		messageData := decompressedChunk.buf[chunkOffset+1+8 : chunkOffset+1+8+length]
		existingbuf := msg.Data
		if err := msg.PopulateFrom(messageData); err != nil {
			return nil, nil, nil, err
		}
		msg.Data = append(existingbuf[:0], msg.Data...)
		it.decompressedChunks[0].unreadMessages--
		it.curMessageIndex++
		channel, ok := it.channels[msg.ChannelID]
		if !ok {
			return nil, nil, nil, fmt.Errorf("message with unrecognized channel ID %d", msg.ChannelID)
		}
		schema, ok := it.schemas[channel.SchemaID]
		if !ok && channel.SchemaID != 0 {
			return nil, nil, nil, fmt.Errorf("channel %d with unrecognized schema ID %d", msg.ChannelID, channel.SchemaID)
		}
		return schema, channel, msg, nil
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
		return it.NextInto(msg)
	}
	chunkOffset := ri.MessageOffsetInChunk
	decompressedChunk := it.decompressedChunks[ri.ChunkSlotIndex]
	length := binary.LittleEndian.Uint64(decompressedChunk.buf[chunkOffset+1:])
	messageData := decompressedChunk.buf[chunkOffset+1+8 : chunkOffset+1+8+length]
	existingbuf := msg.Data
	if err := msg.PopulateFrom(messageData); err != nil {
		return nil, nil, nil, err
	}
	msg.Data = append(existingbuf[:0], msg.Data...)
	it.decompressedChunks[ri.ChunkSlotIndex].unreadMessages--
	channel, ok := it.channels[msg.ChannelID]
	if !ok {
		return nil, nil, nil, fmt.Errorf("message with unrecognized channel ID %d", msg.ChannelID)
	}
	schema, ok := it.schemas[channel.SchemaID]
	if !ok && channel.SchemaID != 0 {
		return nil, nil, nil, fmt.Errorf("channel %d with unrecognized schema ID %d", msg.ChannelID, channel.SchemaID)
	}
	return schema, channel, msg, nil
}

func (it *indexedMessageIterator) Next(buf []byte) (*Schema, *Channel, *Message, error) {
	msg := &Message{Data: buf}
	return it.NextInto(msg)
}

// returns the sum of two uint64s, with a boolean indicating if the sum overflowed.
func checkedAdd(a, b uint64) (uint64, bool) {
	res, carry := bits.Add64(a, b, 0)
	return res, carry != 0
}
