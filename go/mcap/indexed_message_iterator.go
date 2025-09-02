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

	"math"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

const (
	chunkBufferGrowthMultiple = 1.2
)

type chunkSlot struct {
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
// This iterator reads in order by maintaining two ordered queues, one for chunk indexes and one
// for message indexes. On every call to NextInto(), the front element of both queues is checked and
// the earlier is used. When a chunk index is first, the chunk is decompressed, indexed, the
// new message indexes are pushed onto the message index queue and sorted.
// When a message index is first, that message is copied out of the decompressed chunk and yielded
// to the caller.
type indexedMessageIterator struct {
	lexer  *Lexer
	rs     io.ReadSeeker
	topics map[string]bool
	start  uint64
	end    uint64
	order  ReadOrder

	info *Info

	channels          slicemap[Channel]
	schemas           slicemap[Schema]
	statistics        *Statistics
	chunkIndexes      []*ChunkIndex
	attachmentIndexes []*AttachmentIndex
	metadataIndexes   []*MetadataIndex
	footer            *Footer
	fileSize          int64

	curChunkIndex   int
	messageIndexes  []messageIndexWithChunkSlot
	curMessageIndex int
	chunkSlots      []chunkSlot

	zstdDecoder           *zstd.Decoder
	lz4Reader             *lz4.Reader
	hasReadSummarySection bool

	recordBuf        []byte
	metadataCallback func(*Metadata) error
}

func (it *indexedMessageIterator) seekTo(offset uint64) error {
	if offset > uint64(math.MaxInt64) {
		return fmt.Errorf("%w: %d > int64 max", ErrBadOffset, offset)
	}
	signedOffset := int64(offset)
	if signedOffset >= it.fileSize {
		return fmt.Errorf("%w: %d past file end %d", ErrBadOffset, offset, it.fileSize)
	}
	_, err := it.rs.Seek(signedOffset, io.SeekStart)
	return err
}

// parseIndexSection parses the index section of the file and populates the
// related fields of the structure. It must be called prior to any of the other
// access methods.
func (it *indexedMessageIterator) parseSummarySection() error {
	const footerStartOffsetFromEnd = 8 + 4 + 8 + 8 // magic, plus 20 bytes footer
	footerStartPos, err := it.rs.Seek(-footerStartOffsetFromEnd, io.SeekEnd)
	if err != nil {
		return err
	}
	it.fileSize = footerStartPos + footerStartOffsetFromEnd
	buf := make([]byte, 8+20)
	_, err = io.ReadFull(it.rs, buf)
	if err != nil {
		return fmt.Errorf("read error: %w", err)
	}
	magic := buf[20:]
	if !bytes.Equal(magic, Magic) {
		return &ErrBadMagic{location: magicLocationEnd, actual: magic}
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
	err = it.seekTo(footer.SummaryStart)
	if err != nil {
		return fmt.Errorf("failed to seek to summary start: %w", err)
	}

	if it.info != nil {
		for _, schema := range it.info.Schemas {
			it.schemas.Set(schema.ID, schema)
		}
		for _, channel := range it.info.Channels {
			if len(it.topics) == 0 || it.topics[channel.Topic] {
				it.channels.Set(channel.ID, channel)
			}
		}
		for _, idx := range it.info.ChunkIndexes {
			// copy by under section
			if (it.end == 0 && it.start == 0) || (idx.MessageStartTime < it.end && idx.MessageEndTime >= it.start) {
				if len(idx.MessageIndexOffsets) == 0 {
					it.chunkIndexes = append(it.chunkIndexes, idx)
					continue
				}
				for chanID := range idx.MessageIndexOffsets {
					if it.channels.Get(chanID) != nil {
						it.chunkIndexes = append(it.chunkIndexes, idx)
						break
					}
				}
			}
		}
		it.attachmentIndexes = it.info.AttachmentIndexes
		it.metadataIndexes = it.info.MetadataIndexes
		it.statistics = it.info.Statistics
		switch it.order {
		case FileOrder:
			sort.Slice(it.chunkIndexes, func(i, j int) bool {
				return it.chunkIndexes[i].ChunkStartOffset < it.chunkIndexes[j].ChunkStartOffset
			})
		case LogTimeOrder:
			sort.Slice(it.chunkIndexes, func(i, j int) bool {
				if it.chunkIndexes[i].MessageStartTime == it.chunkIndexes[j].MessageStartTime {
					return it.chunkIndexes[i].ChunkStartOffset < it.chunkIndexes[j].ChunkStartOffset
				}
				return it.chunkIndexes[i].MessageStartTime < it.chunkIndexes[j].MessageStartTime
			})
		case ReverseLogTimeOrder:
			sort.Slice(it.chunkIndexes, func(i, j int) bool {
				if it.chunkIndexes[i].MessageEndTime == it.chunkIndexes[j].MessageEndTime {
					return it.chunkIndexes[i].ChunkStartOffset > it.chunkIndexes[j].ChunkStartOffset
				}
				return it.chunkIndexes[i].MessageEndTime > it.chunkIndexes[j].MessageEndTime
			})
		}
		it.hasReadSummarySection = true
		return nil
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
			it.schemas.Set(schema.ID, schema)
		case TokenChannel:
			channelInfo, err := ParseChannel(record)
			if err != nil {
				return fmt.Errorf("failed to parse channel info: %w", err)
			}
			if len(it.topics) == 0 || it.topics[channelInfo.Topic] {
				it.channels.Set(channelInfo.ID, channelInfo)
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
			if (it.end == 0 && it.start == 0) || (idx.MessageStartTime < it.end && idx.MessageEndTime >= it.start) {
				// Can't infer absence of a topic if there are no message indexes.
				if len(idx.MessageIndexOffsets) == 0 {
					it.chunkIndexes = append(it.chunkIndexes, idx)
					continue
				}
				// Otherwise, scan the message index offsets and see if we are
				// selecting it. ChannelInfo is set only for selected topics.
				// NB: It would be nice if we had a more compact/direct
				// representation of what channels are in a chunk.
				for chanID := range idx.MessageIndexOffsets {
					if it.channels.Get(chanID) != nil {
						it.chunkIndexes = append(it.chunkIndexes, idx)
						break
					}
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
					if it.chunkIndexes[i].MessageStartTime == it.chunkIndexes[j].MessageStartTime {
						return it.chunkIndexes[i].ChunkStartOffset < it.chunkIndexes[j].ChunkStartOffset
					}
					return it.chunkIndexes[i].MessageStartTime < it.chunkIndexes[j].MessageStartTime
				})
			case ReverseLogTimeOrder:
				sort.Slice(it.chunkIndexes, func(i, j int) bool {
					if it.chunkIndexes[i].MessageEndTime == it.chunkIndexes[j].MessageEndTime {
						return it.chunkIndexes[i].ChunkStartOffset > it.chunkIndexes[j].ChunkStartOffset
					}
					return it.chunkIndexes[i].MessageEndTime > it.chunkIndexes[j].MessageEndTime
				})
			}
			it.hasReadSummarySection = true
			return nil
		}
	}
}

// loadChunk seeks to and decompresses a chunk into a chunk slot, then populates it.messageIndexes
// with the offsets of messages in that chunk.
func (it *indexedMessageIterator) loadChunk(chunkIndex *ChunkIndex) error {
	err := it.seekTo(chunkIndex.ChunkStartOffset)
	if err != nil {
		return err
	}

	compressedChunkLength := chunkIndex.ChunkLength
	if uint64(cap(it.recordBuf)) < compressedChunkLength {
		newCapacity := int(float64(compressedChunkLength) * chunkBufferGrowthMultiple)
		it.recordBuf = make([]byte, compressedChunkLength, newCapacity)
	} else {
		it.recordBuf = it.recordBuf[:compressedChunkLength]
	}
	_, err = io.ReadFull(it.rs, it.recordBuf)
	if err != nil {
		return fmt.Errorf("failed to read chunk data: %w", err)
	}
	parsedChunk, err := ParseChunk(it.recordBuf[9:])
	if err != nil {
		return fmt.Errorf("failed to parse chunk: %w", err)
	}
	// decompress the chunk data
	chunkSlotIndex := -1
	for i, chunkSlot := range it.chunkSlots {
		if chunkSlot.unreadMessages == 0 {
			chunkSlotIndex = i
			break
		}
	}
	if chunkSlotIndex == -1 {
		it.chunkSlots = append(it.chunkSlots, chunkSlot{})
		chunkSlotIndex = len(it.chunkSlots) - 1
	}
	chunkSlot := &it.chunkSlots[chunkSlotIndex]
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
	// Always assume we need to sort newly added message indexes unless there are no outstanding
	// messages. Even though chunks indexes are in order of messageStartTime, the message indexes
	// within them may overlap or be out of order (especially when time/topic filters are considered).
	sortingRequired := true
	// if there are no message indexes outstanding, truncate now.
	if it.curMessageIndex == len(it.messageIndexes) {
		it.curMessageIndex = 0
		it.messageIndexes = it.messageIndexes[:0]
		sortingRequired = false
	}
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
		if op == OpMessage {
			msg := Message{}
			if err := msg.PopulateFrom(recordContent, false); err != nil {
				return fmt.Errorf("could not parse message in chunk: %w", err)
			}
			if it.channels.Get(msg.ChannelID) != nil {
				if msg.LogTime >= it.start && msg.LogTime < it.end {
					it.messageIndexes = append(it.messageIndexes, messageIndexWithChunkSlot{
						timestamp:      msg.LogTime,
						offset:         offset,
						chunkSlotIndex: chunkSlotIndex,
					})
					if msg.LogTime < maxLogTime {
						sortingRequired = true
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
		if sortingRequired {
			// We stable-sort to ensure that if messages in different chunks have the
			// same timestamp, the one from the earlier-loaded chunk is returned first. The offset
			// field of the message index is not comparable between indexes of different chunks.
			sort.SliceStable(unreadMessageIndexes, func(i, j int) bool {
				return unreadMessageIndexes[i].timestamp < unreadMessageIndexes[j].timestamp
			})
		}
	case ReverseLogTimeOrder:
		// assume message indexes will always be mostly-in-order, so reversing the newly-added
		// indexes will put them mostly into reverse order, which speeds up sorting.
		// If the chunk is in order, no sorting is needed after reversing.
		slices.Reverse(it.messageIndexes[startIdx:])
		if sortingRequired {
			sort.SliceStable(unreadMessageIndexes, func(i, j int) bool {
				return unreadMessageIndexes[i].timestamp > unreadMessageIndexes[j].timestamp
			})
		}
	}
	// if there is more dead space at the front than there is live, remove the dead space by
	// copying the live data to the front and truncating.
	if len(unreadMessageIndexes) < it.curMessageIndex {
		copy(it.messageIndexes[:len(unreadMessageIndexes)], unreadMessageIndexes)
		it.messageIndexes = it.messageIndexes[:len(unreadMessageIndexes)]
		it.curMessageIndex = 0
	}

	return nil
}

func readRecord(r io.Reader, buf []byte) (OpCode, []byte, error) {
	if cap(buf) < 9 {
		buf = make([]byte, 9)
	} else {
		buf = buf[:9]
	}
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read record header: %w", err)
	}
	opcode := OpCode(buf[0])
	recordLen := binary.LittleEndian.Uint64(buf[1:])
	if uint64(cap(buf)) < recordLen {
		buf = make([]byte, recordLen)
	} else {
		buf = buf[:recordLen]
	}
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read record: %w", err)
	}
	return opcode, buf, nil
}

// NextInto yields the next message from the iterator, writing the result into the provided Message
// struct. The msg.Data buffer will be reused if it has enough capacity. If `msg` is nil, a new
// Message will be allocated.
func (it *indexedMessageIterator) NextInto(msg *Message) (*Schema, *Channel, *Message, error) {
	if msg == nil {
		msg = &Message{}
	}
	if !it.hasReadSummarySection {
		if err := it.parseSummarySection(); err != nil {
			return nil, nil, nil, err
		}
		// take care of the metadata here
		if it.metadataCallback != nil {
			for _, idx := range it.metadataIndexes {
				err := it.seekTo(idx.Offset)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("failed to seek to metadata: %w", err)
				}
				opcode, data, err := readRecord(it.rs, it.recordBuf)
				if cap(data) > cap(it.recordBuf) {
					it.recordBuf = data
				}
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
	for {
		// if there are no indexed messages to yield, load a chunk
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
			continue
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
				continue
			}
		}
		// yield the next message
		messageIndex := it.messageIndexes[it.curMessageIndex]
		chunkSlot := &it.chunkSlots[messageIndex.chunkSlotIndex]
		messageDataStart, overflow := checkedAdd(messageIndex.offset, 1+8)
		if overflow {
			return nil, nil, nil, fmt.Errorf("message offset in chunk too close to uint64 max: %d", messageIndex.offset)
		}
		length := binary.LittleEndian.Uint64(chunkSlot.buf[messageIndex.offset+1:])
		messageDataEnd, overflow := checkedAdd(messageDataStart, length)
		if overflow {
			return nil, nil, nil, fmt.Errorf("message record length extends past uint64 range: %d", length)
		}
		messageData := chunkSlot.buf[messageDataStart:messageDataEnd]
		if err := msg.PopulateFrom(messageData, true); err != nil {
			return nil, nil, nil, err
		}
		chunkSlot.unreadMessages--
		it.curMessageIndex++
		channel := it.channels.Get(msg.ChannelID)
		if channel == nil {
			return nil, nil, nil, fmt.Errorf("message with unrecognized channel ID %d", msg.ChannelID)
		}
		schema := it.schemas.Get(channel.SchemaID)
		if schema == nil && channel.SchemaID != 0 {
			return nil, nil, nil, fmt.Errorf("channel %d with unrecognized schema ID %d", msg.ChannelID, channel.SchemaID)
		}
		return schema, channel, msg, nil
	}
}

func (it *indexedMessageIterator) Next(buf []byte) (*Schema, *Channel, *Message, error) {
	msg := &Message{Data: buf}
	return it.NextInto(msg)
}

func (it *indexedMessageIterator) SetBaseInfo(info *Info) error {
	if info == nil {
		return fmt.Errorf("info is nil")
	}
	it.info = info
	return nil
}

// returns the sum of two uint64s, with a boolean indicating if the sum overflowed.
func checkedAdd(a, b uint64) (uint64, bool) {
	res, carry := bits.Add64(a, b, 0)
	return res, carry != 0
}
