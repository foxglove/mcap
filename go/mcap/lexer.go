package mcap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type countingReader struct {
	r   io.Reader
	pos int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.pos += int64(n)
	return n, err
}

// ErrNestedChunk indicates the lexer has detected a nested chunk.
var ErrNestedChunk = errors.New("detected nested chunk")
var ErrChunkTooLarge = errors.New("chunk exceeds configured maximum size")
var ErrRecordTooLarge = errors.New("record exceeds configured maximum size")
var ErrInvalidZeroOpcode = errors.New("invalid zero opcode")

type errInvalidChunkCrc struct {
	expected uint32
	actual   uint32
}

func (e *errInvalidChunkCrc) Error() string {
	return fmt.Sprintf("invalid chunk CRC: %x != %x", e.actual, e.expected)
}

type ErrTruncatedRecord struct {
	opcode      OpCode
	actualLen   int
	expectedLen uint64
}

func (e *ErrTruncatedRecord) Error() string {
	if e.expectedLen == 0 {
		return fmt.Sprintf(
			"MCAP truncated in record length field after %s opcode (%x), received %d bytes",
			e.opcode.String(),
			byte(e.opcode),
			e.actualLen,
		)
	}
	return fmt.Sprintf(
		"MCAP truncated in %s (0x%x) record content with expected length %d, data ended after %d bytes",
		e.opcode.String(),
		byte(e.opcode),
		e.expectedLen,
		e.actualLen,
	)
}

func (e *ErrTruncatedRecord) Unwrap() error {
	return io.ErrUnexpectedEOF
}

type magicLocation int

const (
	magicLocationStart magicLocation = iota
	magicLocationEnd
)

func (m magicLocation) String() string {
	switch m {
	case magicLocationStart:
		return "start"
	case magicLocationEnd:
		return "end"
	default:
		return "unknown"
	}
}

// ErrBadMagic indicates invalid magic bytes were detected.
type ErrBadMagic struct {
	location magicLocation
	actual   []byte
}

func (e *ErrBadMagic) Error() string {
	return fmt.Sprintf("Invalid magic at %s of file, found: %v", e.location, e.actual)
}

func (e *ErrBadMagic) Is(err error) bool {
	_, ok := err.(*ErrBadMagic)
	return ok
}

const (
	// TokenHeader represents a header token.
	TokenHeader TokenType = iota
	// TokenFooter represents a footer token.
	TokenFooter
	// TokenSchema represents a schema token.
	TokenSchema
	// TokenChannel represents a channel token.
	TokenChannel
	// TokenMessage represents a message token.
	TokenMessage
	// TokenChunk represents a chunk token.
	TokenChunk
	// TokenMessageIndex represents a message index token.
	TokenMessageIndex
	// TokenChunkIndex represents a chunk index token.
	TokenChunkIndex
	// TokenAttachmentIndex represents an attachment index token.
	TokenAttachmentIndex
	// TokenStatistics represents a statistics token.
	TokenStatistics
	// TokenMetadata represents a metadata token.
	TokenMetadata
	// TokenSummaryOffset represents a summary offset token.
	TokenMetadataIndex
	// TokenDataEnd represents a data end token.
	TokenSummaryOffset
	// 	TokenError represents an error token.
	TokenDataEnd
	// TokenError represents an error token.
	TokenError
	// TokenInvalidChunk represents a chunk token that failed CRC validation.
	TokenInvalidChunk
)

// TokenType encodes a type of token from the lexer.
type TokenType int

// String converts a token type to its string representation.
func (t TokenType) String() string {
	switch t {
	case TokenHeader:
		return "header"
	case TokenFooter:
		return "footer"
	case TokenSchema:
		return "schema"
	case TokenChannel:
		return "channel"
	case TokenMessage:
		return "message"
	case TokenChunk:
		return "chunk"
	case TokenMessageIndex:
		return "message index"
	case TokenChunkIndex:
		return "chunk index"
	case TokenAttachmentIndex:
		return "attachment index"
	case TokenStatistics:
		return "statistics"
	case TokenMetadata:
		return "metadata"
	case TokenSummaryOffset:
		return "summary offset"
	case TokenDataEnd:
		return "data end"
	case TokenError:
		return "error"
	case TokenInvalidChunk:
		return "invalid chunk"
	default:
		return "unknown"
	}
}

// Lexer is a low-level reader for mcap files that emits tokenized byte strings
// without parsing or interpreting them, except in the case of chunks, which may
// be optionally de-chunked.
type Lexer struct {
	basereader *countingReader
	reader     *countingReader
	emitChunks bool

	lastOffset RecordOffset

	decoders                 decoders
	inChunk                  bool
	buf                      []byte
	uncompressedChunk        []byte
	validateChunkCRCs        bool
	computeAttachmentCRCs    bool
	emitInvalidChunks        bool
	maxRecordSize            int
	maxDecompressedChunkSize int
	attachmentCallback       func(*AttachmentReader) error
	decompressors            map[CompressionFormat]ResettableReader
}

// GetLastTokenOffset returns the TokenOffset of the last token returned from Next().
func (l *Lexer) GetLastTokenOffset() RecordOffset {
	return l.lastOffset
}

// Next returns the next token from the lexer as a byte array. The result will
// be sliced out of the provided buffer `p`, if p has adequate space. If p does
// not have adequate space, a new buffer with sufficient size is allocated for
// the result.
func (l *Lexer) Next(p []byte) (TokenType, []byte, error) {
	for {
		if !l.inChunk {
			l.lastOffset.FileOffset = l.reader.pos
			l.lastOffset.ChunkOffset = RecordNotInChunk
		} else {
			l.lastOffset.ChunkOffset = l.reader.pos
		}
		readLength, err := io.ReadFull(l.reader, l.buf[:9])
		if err != nil {
			unexpectedEOF := errors.Is(err, io.ErrUnexpectedEOF)
			eof := errors.Is(err, io.EOF)
			if l.inChunk && (eof || unexpectedEOF) {
				l.inChunk = false
				l.reader = l.basereader
				continue
			}
			if unexpectedEOF {
				if readLength == len(Magic) && bytes.Equal(Magic, l.buf[:len(Magic)]) {
					return TokenError, nil, io.EOF
				}
				// unexpectedEOF indicates at least one byte was read
				opcode := OpCode(l.buf[0])
				return TokenError, nil, &ErrTruncatedRecord{opcode: opcode, actualLen: readLength}
			}
			return TokenError, nil, err
		}
		opcode := OpCode(l.buf[0])
		recordLen := binary.LittleEndian.Uint64(l.buf[1:9])
		if l.maxRecordSize > 0 && recordLen > uint64(l.maxRecordSize) {
			return TokenError, nil, ErrRecordTooLarge
		}

		// Chunks and attachments require special handling to avoid
		// materialization into RAM. If it's a chunk, open up a decompressor and
		// swap it in as the active reader, then continue on the next message
		// (which will be from the chunk data). If it's an attachment, parse the
		// record into an AttachmentReader and call any user-supplied callback.
		// Then discard any remaining data and continue to the next record.
		switch opcode {
		case OpChunk:
			if !l.emitChunks {
				err := loadChunk(l, recordLen)
				if err != nil {
					if l.emitInvalidChunks {
						var invalidCrc *errInvalidChunkCrc
						if errors.As(err, &invalidCrc) {
							return TokenInvalidChunk, nil, err
						}
					}
					return TokenError, nil, err
				}
				continue
			}
		case OpAttachment:
			limitReader := &io.LimitedReader{
				R: l.reader,
				N: int64(recordLen),
			}

			if l.attachmentCallback != nil {
				attachmentReader, err := parseAttachmentReader(
					limitReader,
					l.computeAttachmentCRCs,
					l.lastOffset,
				)
				if err != nil {
					return TokenError, nil, fmt.Errorf("failed to parse attachment: %w", err)
				}
				err = l.attachmentCallback(attachmentReader)
				if err != nil {
					return TokenError, nil, fmt.Errorf("failed to handle attachment: %w", err)
				}
			}

			// skip the base reader ahead to cover any unconsumed bytes of the attachment
			err := skipReader(limitReader.R, limitReader.N)
			if err != nil {
				return TokenError, nil, fmt.Errorf("failed to consume unhandled attachment data: %w", err)
			}
			continue
		}

		if recordLen > uint64(len(p)) {
			p, err = makeSafe(recordLen)
			if err != nil {
				return TokenError, nil, fmt.Errorf("failed to allocate %d bytes for %s token: %w", recordLen, opcode, err)
			}
		}

		record := p[:recordLen]
		readLength, err = io.ReadFull(l.reader, record)
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return TokenError, nil, &ErrTruncatedRecord{
				opcode:      opcode,
				actualLen:   readLength,
				expectedLen: recordLen,
			}
		}
		if err != nil {
			return TokenError, nil, err
		}

		switch opcode {
		case OpMessage:
			return TokenMessage, record, nil
		case OpHeader:
			return TokenHeader, record, nil
		case OpSchema:
			return TokenSchema, record, nil
		case OpDataEnd:
			return TokenDataEnd, record, nil
		case OpChannel:
			return TokenChannel, record, nil
		case OpFooter:
			return TokenFooter, record, nil
		case OpAttachmentIndex:
			return TokenAttachmentIndex, record, nil
		case OpChunkIndex:
			return TokenChunkIndex, record, nil
		case OpStatistics:
			return TokenStatistics, record, nil
		case OpMessageIndex:
			return TokenMessageIndex, record, nil
		case OpChunk:
			return TokenChunk, record, nil
		case OpMetadata:
			return TokenMetadata, record, nil
		case OpMetadataIndex:
			return TokenMetadataIndex, record, nil
		case OpSummaryOffset:
			return TokenSummaryOffset, record, nil
		case OpReserved:
			return TokenError, nil, ErrInvalidZeroOpcode
		default:
			continue // skip unrecognized opcodes
		}
	}
}

// Close the lexer.
func (l *Lexer) Close() {
	if l.decoders.zstd != nil {
		l.decoders.zstd.Close()
	}
}

type decoders struct {
	zstd *zstd.Decoder
	lz4  *lz4.Reader
	none *bytes.Reader
}

func validateMagic(r io.Reader, location magicLocation) error {
	magic := make([]byte, len(Magic))
	if readLen, err := io.ReadFull(r, magic); err != nil {
		return &ErrBadMagic{actual: magic[:readLen], location: location}
	}
	if !bytes.Equal(magic, Magic) {
		return &ErrBadMagic{actual: magic, location: location}
	}
	return nil
}

func (l *Lexer) setNoneDecoder(buf []byte) {
	if l.decoders.none == nil {
		l.decoders.none = bytes.NewReader(buf)
	} else {
		l.decoders.none.Reset(buf)
	}
	l.reader = &countingReader{r: l.decoders.none}
}

func (l *Lexer) setZSTDDecoder(r io.Reader) error {
	if l.decoders.zstd == nil {
		decoder, err := zstd.NewReader(r)
		if err != nil {
			return err
		}
		l.decoders.zstd = decoder
	} else {
		err := l.decoders.zstd.Reset(r)
		if err != nil {
			return err
		}
	}
	l.reader = &countingReader{r: l.decoders.zstd}
	return nil
}

func (l *Lexer) setLZ4Decoder(r io.Reader) {
	if l.decoders.lz4 == nil {
		l.decoders.lz4 = lz4.NewReader(r)
	} else {
		l.decoders.lz4.Reset(r)
	}
	l.reader = &countingReader{r: l.decoders.lz4}
}

func loadChunk(l *Lexer, recordLen uint64) error {
	if l.inChunk {
		return ErrNestedChunk
	}
	readLength, err := io.ReadFull(l.reader, l.buf[:8+8+8+4+4])
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return &ErrTruncatedRecord{
			opcode:      OpChunk,
			expectedLen: recordLen,
			actualLen:   readLength,
		}
	}
	if err != nil {
		return err
	}

	_, offset, err := getUint64(l.buf, 0) // start
	if err != nil {
		return fmt.Errorf("failed to read start: %w", err)
	}
	_, offset, err = getUint64(l.buf, offset) // end
	if err != nil {
		return fmt.Errorf("failed to read end: %w", err)
	}
	uncompressedSize, offset, err := getUint64(l.buf, offset)
	if err != nil {
		return fmt.Errorf("failed to read uncompressed size: %w", err)
	}
	uncompressedCRC, offset, err := getUint32(l.buf, offset)
	if err != nil {
		return fmt.Errorf("failed to read uncompressed CRC: %w", err)
	}
	compressionLen, _, err := getUint32(l.buf, offset)
	if err != nil {
		return fmt.Errorf("failed to read compression length: %w", err)
	}

	// read compression and records length into buffer
	thisReadLength, err := io.ReadFull(l.reader, l.buf[:compressionLen+8])
	readLength += thisReadLength
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return &ErrTruncatedRecord{
			opcode:      OpChunk,
			expectedLen: recordLen,
			actualLen:   readLength,
		}
	}
	if err != nil {
		return fmt.Errorf("failed to read compression from chunk: %w", err)
	}
	compression := CompressionFormat(l.buf[:compressionLen])
	recordsLength, _, err := getUint64(l.buf, int(compressionLen))
	if err != nil {
		return fmt.Errorf("failed to read records length: %w", err)
	}

	// remaining bytes in the record are the chunk data
	lr := io.LimitReader(l.reader, int64(recordsLength))
	switch {
	case l.decompressors[compression] != nil: // must be top
		decoder := l.decompressors[compression]
		err = decoder.Reset(lr)
		if err != nil {
			return fmt.Errorf("failed to reset custom decompressor: %w", err)
		}
		l.reader = &countingReader{r: decoder}
	case compression == CompressionNone:
		l.reader = &countingReader{r: lr}
	case compression == CompressionZSTD:
		err = l.setZSTDDecoder(lr)
		if err != nil {
			return err
		}
	case compression == CompressionLZ4:
		l.setLZ4Decoder(lr)
	default:
		return fmt.Errorf("unsupported compression: %s", string(compression))
	}
	l.inChunk = true

	// if we are validating the CRC, we need to fully decompress the chunk right
	// here, then rewrap the decompressed data in a compatible reader after
	// validation. If we are not validating CRCs, we can use incremental
	// decompression for the chunk's data, which may be beneficial to streaming
	// readers.
	if l.validateChunkCRCs {
		if l.maxDecompressedChunkSize > 0 && uncompressedSize > uint64(l.maxDecompressedChunkSize) {
			return ErrChunkTooLarge
		}
		if uint64(len(l.uncompressedChunk)) < uncompressedSize {
			l.uncompressedChunk, err = makeSafe(uncompressedSize * 2)
			if err != nil {
				return fmt.Errorf("failed to allocate chunk buffer: %w", err)
			}
		}

		_, err := io.ReadFull(l.reader, l.uncompressedChunk[:uncompressedSize])
		if err != nil {
			return fmt.Errorf("failed to decompress chunk: %w", err)
		}

		// LZ4 chunks may have some crc data at the end that is not required to
		// fill a buffer, meaning the ReadFull call above does not consume it.
		// Therefore we have to do an empty read. If we get any data out of
		// this, it's an error.
		if compression == CompressionLZ4 {
			extraBytes, err := io.ReadAll(l.reader)
			if err != nil {
				return fmt.Errorf("failed to read extra bytes: %w", err)
			}
			if len(extraBytes) > 0 {
				return fmt.Errorf("encountered unexpected bytes after chunk: %q", extraBytes)
			}
		}

		crc := crc32.ChecksumIEEE(l.uncompressedChunk[:uncompressedSize])
		if uncompressedCRC > 0 && crc != uncompressedCRC {
			return &errInvalidChunkCrc{expected: uncompressedCRC, actual: crc}
		}
		l.setNoneDecoder(l.uncompressedChunk[:uncompressedSize])
	}
	return nil
}

// LexerOptions holds options for the lexer.
type LexerOptions struct {
	// SkipMagic instructs the lexer not to perform validation of the leading magic bytes.
	SkipMagic bool
	// ValidateChunkCRC instructs the lexer to validate CRC checksums for
	// chunks.
	ValidateChunkCRCs bool
	// ComputeAttachmentCRCs instructs the lexer to compute CRCs for any
	// attachments parsed from the file. Consumers should only set this to true
	// if they intend to validate those CRCs in their attachment callback.
	ComputeAttachmentCRCs bool
	// EmitChunks instructs the lexer to emit chunk records without de-chunking.
	// It is incompatible with ValidateCRC.
	EmitChunks bool
	// EmitChunks instructs the lexer to emit TokenInvalidChunk rather than TokenError when CRC
	// validation fails.
	EmitInvalidChunks bool
	// MaxDecompressedChunkSize defines the maximum size chunk the lexer will
	// decompress. Chunks larger than this will result in an error.
	MaxDecompressedChunkSize int
	// MaxRecordSize defines the maximum size record the lexer will read.
	// Records larger than this will result in an error.
	MaxRecordSize int
	// AttachmentCallback is a function to execute on attachments encountered in the file.
	AttachmentCallback func(*AttachmentReader) error
	// Decompressors are custom decompressors. Chunks matching the supplied
	// compression format will be decompressed with the provided
	// ResettableReader instead of the default implementation.
	Decompressors map[CompressionFormat]ResettableReader
}

// NewLexer returns a new lexer for the given reader.
func NewLexer(r io.Reader, opts ...*LexerOptions) (*Lexer, error) {
	var maxRecordSize, maxDecompressedChunkSize int
	var computeAttachmentCRCs, validateChunkCRCs, emitChunks, emitInvalidChunks, skipMagic bool
	var attachmentCallback func(*AttachmentReader) error
	var decompressors map[CompressionFormat]ResettableReader
	if len(opts) > 0 {
		validateChunkCRCs = opts[0].ValidateChunkCRCs
		computeAttachmentCRCs = opts[0].ComputeAttachmentCRCs
		emitChunks = opts[0].EmitChunks
		emitInvalidChunks = opts[0].EmitInvalidChunks
		skipMagic = opts[0].SkipMagic
		maxRecordSize = opts[0].MaxRecordSize
		maxDecompressedChunkSize = opts[0].MaxDecompressedChunkSize
		attachmentCallback = opts[0].AttachmentCallback
		decompressors = opts[0].Decompressors
	}
	basereader := &countingReader{r: r}
	if !skipMagic {
		err := validateMagic(basereader, magicLocationStart)
		if err != nil {
			return nil, err
		}
	}

	return &Lexer{
		basereader:               basereader,
		reader:                   basereader,
		buf:                      make([]byte, 32),
		validateChunkCRCs:        validateChunkCRCs,
		computeAttachmentCRCs:    computeAttachmentCRCs,
		emitChunks:               emitChunks,
		emitInvalidChunks:        emitInvalidChunks,
		maxRecordSize:            maxRecordSize,
		maxDecompressedChunkSize: maxDecompressedChunkSize,
		attachmentCallback:       attachmentCallback,
		decompressors:            decompressors,
	}, nil
}
