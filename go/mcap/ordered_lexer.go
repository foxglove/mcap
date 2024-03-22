package mcap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

var ErrWouldExceedMemoryLimit = errors.New("input requires too large of a message buffer")
var ErrInsufficientSummary = errors.New("input does not contain required summary information")
var ErrChunksNotIndexed = fmt.Errorf("%w: need chunk indexes", ErrInsufficientSummary)
var ErrNoStatistics = fmt.Errorf("%w: need a statistics record", ErrInsufficientSummary)

type recordIndex struct {
	token     TokenType
	timestamp uint64
	offset    uint64
	length    uint64
}

// OrderedLexer reads MCAP files record by record, yielding message records in ascending logTime
// order.
type OrderedLexer struct {
	chunkContent   []byte
	recordBuf      []byte
	recordIndexes  []recordIndex
	curRecordIndex int

	chunkLoader chunkLoader

	chunkTags            []chunkTag
	curChunk             int
	readyToYieldMessages bool
	lexer                *Lexer
}

type chunkTag struct {
	uncompressedSize uint64
	mergeWithNext    bool
}

// tagChunksForMerging produces a chunkTag for every ChunkIndex in `info`. This tag determines
// whether that chunk must be merged with one or more after it during reading.
func tagChunksForMerging(info *Info) []chunkTag {
	// ensure chunk indexes are sorted in physical order
	sort.Slice(info.ChunkIndexes, func(i, j int) bool {
		return info.ChunkIndexes[i].ChunkStartOffset < info.ChunkIndexes[j].ChunkStartOffset
	})

	chunkTags := make([]chunkTag, len(info.ChunkIndexes))
	mergeUpTo := 0
	for i, ci := range info.ChunkIndexes {
		chunkTags[i].uncompressedSize = ci.UncompressedSize
		// find the last chunk index that overlaps with this one, and ensure that this chunk
		// and all between are merged into it.
		for j := i + 1; j < len(info.ChunkIndexes); j++ {
			if info.ChunkIndexes[j].MessageStartTime < ci.MessageEndTime {
				if j > mergeUpTo {
					mergeUpTo = j
				}
			}
		}
		if i < mergeUpTo {
			chunkTags[i].mergeWithNext = true
		}
	}
	return chunkTags
}

// getMaxRecordSize determines the largest single record which will be read from this MCAP during
// lexing. This is used to pre-allocate the record buffer that the lexer reads into.
func getMaxRecordSize(info *Info) uint64 {
	// assume chunks are the largest records that we'll have to buffer.
	realChunkSize := func(chunkIndex *ChunkIndex) uint64 {
		// https://dev/spec#chunk-op0x06
		return 8 + 8 + 8 + 4 + 4 + uint64(len(chunkIndex.Compression)) + 8 + chunkIndex.CompressedSize
	}
	var maxChunkRecordSize uint64
	for _, chunkIndex := range info.ChunkIndexes {
		size := realChunkSize(chunkIndex)
		if size > maxChunkRecordSize {
			maxChunkRecordSize = size
		}
	}
	return maxChunkRecordSize
}

// NewOrderedLexer constructs a reader. It inspects the summary section of the MCAP, checking for
// overlapping chunks. For any range of chunks that overlap in time range, they are tagged as
// needing to be merged together in order to be sorted. It then determines the size of chunk buffer
// required, and returns an error if this is greater than `maxBufferSize`.
func NewOrderedLexer(
	r io.Reader,
	info *Info,
	maxBufferSize uint64,
	attachmentCallback func(*AttachmentReader) error,
) (*OrderedLexer, error) {
	if err := checkIndexes(info); err != nil {
		return nil, err
	}

	chunkTags := tagChunksForMerging(info)

	requiredBufferSize := determineRequiredBufferSize(chunkTags)
	if requiredBufferSize > maxBufferSize {
		return nil, ErrWouldExceedMemoryLimit
	}
	lexer, err := NewLexer(r, &LexerOptions{EmitChunks: true, AttachmentCallback: attachmentCallback})
	if err != nil {
		return nil, err
	}
	return &OrderedLexer{
		chunkContent: make([]byte, 0, requiredBufferSize),
		recordBuf:    make([]byte, getMaxRecordSize(info)),
		chunkTags:    chunkTags,
		lexer:        lexer,
	}, nil
}

// checkIndexes determines whether the information in the MCAP summary is sufficient to read
// messages in order.
func checkIndexes(info *Info) error {
	if info.Statistics == nil {
		return ErrNoStatistics
	}
	if len(info.ChunkIndexes) == 0 && info.Statistics.MessageCount != 0 {
		return ErrChunksNotIndexed
	}
	return nil
}

// determineRequiredBufferSize sums up the uncompressed size of chunks that need to be merged
// together, and from there determines the maximum chunk buffer size needed to read this file
// in log time order.
func determineRequiredBufferSize(chunkTags []chunkTag) uint64 {
	var maxBufferSize uint64
	for i := len(chunkTags) - 1; i >= 0; i-- {
		chunkTag := chunkTags[i]
		if chunkTag.mergeWithNext {
			maxBufferSize += chunkTag.uncompressedSize
		} else {
			maxBufferSize = chunkTag.uncompressedSize
		}
	}
	return maxBufferSize
}

// Next yields the next record from the reader. If none remain, returns io.EOF.
//
// Initially, records are read from the MCAP with an Lexer, and are yielded immediately.
// When chunk records are found, these are decompressed into `chunkContent`, and a sorted array
// of recordIndex structs is maintained for every record in the `chunkContent` buffer.
// If the content of the chunk buffer needs to be merged with more chunks, the lexer continues
// until all required chunks are loaded into `chunkContent`. Then, the OrderedLexer switches
// to yielding records from the chunk buffer until it is exhausted, at which point it switches back.
func (ol *OrderedLexer) Next([]byte) (TokenType, []byte, error) {
	for {
		if ol.readyToYieldMessages {
			if ol.curRecordIndex < len(ol.recordIndexes) {
				recordIndex := ol.recordIndexes[ol.curRecordIndex]
				ol.curRecordIndex++
				buf := ol.chunkContent[recordIndex.offset : recordIndex.offset+recordIndex.length]
				return recordIndex.token, buf, nil
			}
			ol.readyToYieldMessages = false
			ol.curRecordIndex = 0
			ol.recordIndexes = ol.recordIndexes[:0]
			ol.chunkContent = ol.chunkContent[:0]
		}
		token, buf, err := ol.lexer.Next(ol.recordBuf)
		if len(buf) > len(ol.recordBuf) {
			ol.recordBuf = buf
		}
		if err != nil {
			return TokenError, nil, err
		}
		switch token {
		case TokenChunk:
			chunk, err := ParseChunk(buf)
			if err != nil {
				return TokenError, nil, err
			}
			newChunkContent, newRecordIndexes, err := ol.chunkLoader.loadChunk(chunk, ol.chunkContent, ol.recordIndexes)
			if err != nil {
				return TokenError, nil, err
			}
			chunkTag := ol.chunkTags[ol.curChunk]
			ol.curChunk++
			ol.chunkContent = newChunkContent
			ol.recordIndexes = newRecordIndexes
			ol.readyToYieldMessages = !chunkTag.mergeWithNext
		default:
			return token, buf, err
		}
	}
}

func (ol *OrderedLexer) Close() {
	if ol.lexer != nil {
		ol.lexer.Close()
	}
	ol.chunkLoader.Close()
}

type chunkLoader struct {
	zstd *zstd.Decoder
	lz4  *lz4.Reader
}

func (l *chunkLoader) getZstdDecoder(r io.Reader) (io.Reader, error) {
	if l.zstd == nil {
		decoder, err := zstd.NewReader(r)
		if err != nil {
			return nil, err
		}
		l.zstd = decoder
	} else {
		err := l.zstd.Reset(r)
		if err != nil {
			return nil, err
		}
	}
	return l.zstd, nil
}

func (l *chunkLoader) getLZ4Decoder(r io.Reader) io.Reader {
	if l.lz4 == nil {
		l.lz4 = lz4.NewReader(r)
	} else {
		l.lz4.Reset(r)
	}
	return l.lz4
}

// loadChunk appends the decompressed records from `chunk` into `intoBuf`, and inserts record
// indexes to `indexes` for the records in this chunk. record indexes are maintained in log-time
// order.
func (l *chunkLoader) loadChunk(
	chunk *Chunk,
	intoBuf []byte,
	indexes []recordIndex,
) ([]byte, []recordIndex, error) {
	// Grow `intoBuf` to fit the new uncompressed records in chunk.
	initialLength := uint64(len(intoBuf))
	newLength := initialLength + chunk.UncompressedSize
	if uint64(cap(intoBuf)) < newLength {
		intoBuf = append(intoBuf, make([]byte, chunk.UncompressedSize)...)
	} else {
		intoBuf = intoBuf[:newLength]
	}

	// remaining bytes in the record are the chunk data
	var chunkReader *crcReader
	computeCrc := chunk.UncompressedCRC != 0
	format := CompressionFormat(chunk.Compression)
	byteReader := bytes.NewReader(chunk.Records)
	switch format {
	case CompressionNone:
		chunkReader = newCRCReader(byteReader, computeCrc)
	case CompressionZSTD:
		cr, err := l.getZstdDecoder(byteReader)
		if err != nil {
			return nil, nil, err
		}
		chunkReader = newCRCReader(cr, computeCrc)
	case CompressionLZ4:
		chunkReader = newCRCReader(l.getLZ4Decoder(byteReader), computeCrc)
	default:
		return nil, nil, fmt.Errorf("unsupported compression: %s", chunk.Compression)
	}
	_, err := io.ReadFull(chunkReader, intoBuf[initialLength:])
	if err != nil {
		return nil, nil, err
	}
	if chunk.UncompressedCRC != 0 && chunkReader.Checksum() != chunk.UncompressedCRC {
		return nil, nil, fmt.Errorf("invalid chunk CRC: expected %d, got %d", chunk.UncompressedCRC, chunkReader.Checksum())
	}

	for i := initialLength; i < newLength; {
		if uint64(len(intoBuf)) < i+1+8 {
			return nil, nil, fmt.Errorf("expected another record in chunk, but left with %d bytes", uint64(len(intoBuf))-i)
		}
		opcodeAndLengthBuf := intoBuf[i : i+1+8]
		op := OpCode(opcodeAndLengthBuf[0])
		recordLen := binary.LittleEndian.Uint64(opcodeAndLengthBuf[1:])
		recordStart := i + 1 + 8
		if uint64(len(intoBuf)) < recordStart+recordLen {
			return nil, nil, fmt.Errorf(
				"%s record in chunk has length %d bytes but only %d remaining in chunk",
				op.String(), recordLen, uint64(len(intoBuf))-recordStart)
		}
		recordContent := intoBuf[recordStart : recordStart+recordLen]
		if uint64(len(recordContent)) < recordLen {
			return nil, nil, fmt.Errorf(
				"expected %d bytes remaining in chunk, for %s record, found %d",
				recordLen,
				op.String(),
				len(recordContent),
			)
		}
		recordIndex := recordIndex{
			offset: recordStart,
			length: recordLen,
		}
		switch op {
		case OpMessage:
			logTime, err := getLogTimeOfMessageSlice(recordContent)
			if err != nil {
				return nil, nil, fmt.Errorf("could not parse message in chunk: %w", err)
			}
			recordIndex.timestamp = logTime
			recordIndex.token = TokenMessage
		case OpChannel:
			recordIndex.token = TokenChannel
		case OpSchema:
			recordIndex.token = TokenSchema
		default:
			return nil, nil, fmt.Errorf("unexpected record type in chunk buffer: %s", op.String())
		}
		indexes = isort(indexes, recordIndex)
		i = recordStart + recordLen
	}
	return intoBuf, indexes, nil
}

func (l *chunkLoader) Close() {
	if l.zstd != nil {
		l.zstd.Close()
	}
}

// Parses the logTime from a buffer containing a Message record.
// This is cheaper than ParseMessage because it does not allocate a new object.
func getLogTimeOfMessageSlice(buf []byte) (uint64, error) {
	// https://dev/spec#message-op0x05
	if len(buf) < 2+4+8 {
		return 0, io.ErrShortBuffer
	}
	return binary.LittleEndian.Uint64(buf[2+4:]), nil
}

// isort inserts a new recordIndex into an already-sorted array of record indexes. The array
// is returned with the new index in order.
func isort(indexes []recordIndex, newIndex recordIndex) []recordIndex {
	pos := len(indexes)
	switch newIndex.token {
	case TokenChannel:
		// place channels before all the messages
		for ; pos > 0; pos-- {
			if indexes[pos-1].token != TokenMessage {
				break
			}
		}
	case TokenSchema:
		// place schemas before all channels and messages
		for ; pos > 0; pos-- {
			if indexes[pos-1].token == TokenSchema {
				break
			}
		}
	case TokenMessage:
		// place messages after all other record types, and messages with equal or lesser timestamp
		for ; pos > 0; pos-- {
			if indexes[pos-1].token != TokenMessage {
				break
			}
			if indexes[pos-1].timestamp <= newIndex.timestamp {
				break
			}
		}
	default:
		panic("invariant: isort caller should ensure all indexed records are channels, messages or schemas")
	}
	return slices.Insert(indexes, pos, newIndex)
}
