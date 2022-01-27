package libmcap

import (
	"bytes"
	"fmt"
	"io"
	"sort"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type messageOffset struct {
	chunkIndex  int
	chunkOffset int
	timestamp   uint64
}

// indexedMessageIterator is an iterator over an indexed mcap read seeker (as
// seeking is required). It makes reads in alternation from the index data
// section, the message index at the end of a chunk, and the chunk's contents.
type indexedMessageIterator struct {
	lexer  *lexer
	rs     io.ReadSeeker
	topics map[string]bool
	start  uint64
	end    uint64

	channels          map[uint16]*ChannelInfo
	statistics        *Statistics
	chunksets         [][]*ChunkIndex
	chunkIndexes      []*ChunkIndex
	attachmentIndexes []*AttachmentIndex

	// current location in the index
	activeChunksetIndex int           // active chunkset
	activeChunkIndex    int           // index of the active chunk within the set
	activeChunkReader   *bytes.Reader // active decompressed chunk
	activeChunkLexer    *lexer
	messageOffsets      []messageOffset
	messageOffsetIdx    int
	buf                 []byte // opcode + len
}

// parseIndexSection parses the index section of the file and populates the
// related fields of the structure. It must be called prior to any of the other
// access methods.
func (it *indexedMessageIterator) parseIndexSection() error {
	_, err := it.rs.Seek(-8-4-8, io.SeekEnd)
	if err != nil {
		return err
	}
	buf := make([]byte, 8+4+8)
	_, err = io.ReadFull(it.rs, buf)
	if err != nil {
		return fmt.Errorf("read error: %s", err)
	}
	indexOffset, offset := getUint64(buf, 0)
	_, offset = getUint32(buf, offset) // crc
	magic := buf[offset:]
	if !bytes.Equal(magic, Magic) {
		return fmt.Errorf("not an mcap file")
	}
	err = it.seekFile(int64(indexOffset))
	if err != nil {
		return err
	}
	var msg []byte
	defer func() {
		it.chunksets = sortOverlappingChunks(it.chunkIndexes)
	}()

	// now we're in the index data section. Read through the statistics record,
	// populating the index fields.
Top:
	for {
		tok, err := it.lexer.Next()
		if err != nil {
			return fmt.Errorf("lexer error: %w", err)
		}
		msg = tok.bytes()
		switch tok.TokenType {
		case TokenChunkIndex:
			chunkIndex, err := parseChunkIndex(msg)
			if err != nil {
				return fmt.Errorf("failed to parse chunk index: %w", err)
			}
			// if the chunk index overlaps with the requested parameters, append to the list
			if chunkIndex.StartTime < it.end && chunkIndex.EndTime >= it.start {
				// if the message index relates to any target channels, include it
				for k, _ := range it.channels {
					if chunkIndex.MessageIndexOffsets[k] != 0 {
						it.chunkIndexes = append(it.chunkIndexes, chunkIndex)
						break
					}
				}
			}
		case TokenAttachmentIndex:
			attachmentIndex, err := parseAttachmentIndex(msg)
			if err != nil {
				return fmt.Errorf("failed to parse attachment index: %w", err)
			}
			it.attachmentIndexes = append(it.attachmentIndexes, attachmentIndex)
		case TokenChannelInfo:
			// if the channel info is one of those requested, add it to our list
			channelInfo, err := parseChannelInfo(msg)
			if err != nil {
				return fmt.Errorf("failed to parse channel info: %w", err)
			}
			if len(it.topics) == 0 || it.topics[channelInfo.TopicName] {
				it.channels[channelInfo.ChannelID] = channelInfo
			}
		case TokenStatistics:
			stats := parseStatisticsRecord(msg)
			it.statistics = stats
			break Top
		default:
			return fmt.Errorf("unexpected token %s in index data section", tok)
		}
	}
	return nil
}

func sortOverlappingChunks(chunkIndexes []*ChunkIndex) [][]*ChunkIndex {
	output := [][]*ChunkIndex{}
	chunkset := []*ChunkIndex{}
	sort.Slice(chunkIndexes, func(i, j int) bool {
		return chunkIndexes[i].StartTime < chunkIndexes[j].StartTime
	})

	var maxend, minstart uint64
	for _, chunkIndex := range chunkIndexes {
		if len(chunkset) == 0 {
			chunkset = append(chunkset, chunkIndex)
			maxend = chunkIndex.EndTime
			minstart = chunkIndex.StartTime
			continue
		}

		// if this chunk index overlaps with the chunkset in hand, add it
		if chunkIndex.EndTime >= minstart && chunkIndex.StartTime < maxend {
			chunkset = append(chunkset, chunkIndex)
			if minstart > chunkIndex.StartTime {
				minstart = chunkIndex.StartTime
			}
			if maxend < chunkIndex.EndTime {
				maxend = chunkIndex.EndTime
			}
			continue
		}

		// else the chunk in hand is not overlapping, so close the chunkset and
		// initialize a new one
		output = append(output, chunkset)
		chunkset = []*ChunkIndex{chunkIndex}
		maxend = chunkIndex.EndTime
		minstart = chunkIndex.StartTime
	}

	if len(chunkset) > 0 {
		output = append(output, chunkset)
	}

	return output
}

func (it *indexedMessageIterator) loadChunk(index int) error {
	chunkset := it.chunksets[it.activeChunksetIndex]
	chunkIndex := chunkset[index]
	err := it.seekFile(int64(chunkIndex.ChunkOffset))
	if err != nil {
		return err
	}
	tok, err := it.lexer.Next()
	if err != nil {
		return err
	}
	var chunk *Chunk
	switch tok.TokenType {
	case TokenChunk:
		chunk, err = parseChunk(tok.bytes())
		if err != nil {
			return fmt.Errorf("failed to parse chunk: %w", err)
		}
	default:
		_ = tok.bytes()
		return fmt.Errorf("unexpected token %s in chunk section", tok)
	}
	switch CompressionFormat(chunk.Compression) {
	case CompressionNone:
		it.activeChunkReader = bytes.NewReader(chunk.Records)
	case CompressionZSTD:
		buf := make([]byte, chunk.UncompressedSize)
		reader, err := zstd.NewReader(bytes.NewReader(chunk.Records))
		if err != nil {
			return err
		}
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return err
		}
		it.activeChunkReader = bytes.NewReader(buf)
	case CompressionLZ4:
		buf := make([]byte, chunk.UncompressedSize)
		reader := lz4.NewReader(bytes.NewReader(chunk.Records))
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return err
		}
		it.activeChunkReader = bytes.NewReader(buf)
	default:
		return fmt.Errorf("unsupported compression format %s", chunk.Compression)
	}

	it.activeChunkIndex = index
	it.activeChunkLexer, err = NewLexer(it.activeChunkReader, &LexOpts{
		SkipMagic: true,
	})
	if err != nil {
		return fmt.Errorf("failed to lex chunk: %s", err)
	}
	return nil
}

func (it *indexedMessageIterator) loadNextChunkset() error {
	it.activeChunksetIndex++
	it.messageOffsets = it.messageOffsets[:0]
	chunkset := it.chunksets[it.activeChunksetIndex]
	for i, chunkIndex := range chunkset {
		for channelID, offset := range chunkIndex.MessageIndexOffsets {
			if _, ok := it.channels[channelID]; !ok {
				continue
			}
			err := it.seekFile(int64(offset))
			if err != nil {
				return err
			}
			// now we're at the message index implicated by the chunk; parse one record
			var messageIndex *MessageIndex
			tok, err := it.lexer.Next()
			if err != nil {
				return err
			}
			switch tok.TokenType {
			case TokenMessageIndex:
				messageIndex = parseMessageIndex(tok.bytes())
			default:
				_ = tok.bytes()
				return fmt.Errorf("unexpected token %s in message index section", tok)
			}
			for _, record := range messageIndex.Records {
				if record.Timestamp >= it.start && record.Timestamp < it.end {
					it.messageOffsets = append(it.messageOffsets, messageOffset{
						chunkIndex:  i,
						chunkOffset: int(record.Offset),
						timestamp:   record.Timestamp,
					})
				}
			}
		}
	}
	sort.Slice(it.messageOffsets, func(i, j int) bool {
		return it.messageOffsets[i].timestamp < it.messageOffsets[j].timestamp
	})
	it.messageOffsetIdx = 0
	return it.loadChunk(0)
}

func (it *indexedMessageIterator) seekFile(offset int64) error {
	_, err := it.rs.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	return nil
}

func (it *indexedMessageIterator) seekChunk(offset int64) error {
	_, err := it.activeChunkReader.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	return nil
}

func (it *indexedMessageIterator) Next() (*ChannelInfo, *Message, error) {
	if it.statistics == nil {
		err := it.parseIndexSection()
		if err != nil {
			return nil, nil, err
		}
	}
	if it.messageOffsetIdx >= len(it.messageOffsets) {
		if it.activeChunksetIndex >= len(it.chunksets)-1 {
			return nil, nil, io.EOF
		}
		err := it.loadNextChunkset()
		if err != nil {
			return nil, nil, err
		}
	}

	messageOffset := it.messageOffsets[it.messageOffsetIdx]
	it.messageOffsetIdx++

	// if this message is on a different chunk within the chunkset, we need to
	// switch to that chunk
	if messageOffset.chunkIndex != it.activeChunkIndex {
		err := it.loadChunk(messageOffset.chunkIndex)
		if err != nil {
			return nil, nil, err
		}
	}

	// now the active chunk matches the one for this message
	err := it.seekChunk(int64(messageOffset.chunkOffset))
	if err != nil {
		return nil, nil, err
	}
	tok, err := it.activeChunkLexer.Next()
	if err != nil {
		return nil, nil, err
	}
	switch tok.TokenType {
	case TokenMessage:
		msg, err := parseMessage(tok.bytes())
		if err != nil {
			return nil, nil, err
		}
		return it.channels[msg.ChannelID], msg, nil
	default:
		_ = tok.bytes()
		return nil, nil, fmt.Errorf("unexpected token %s in message section", tok)
	}
}
