package libmcap

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

var (
	ErrNestedChunk = errors.New("detected nested chunk")
	ErrBadMagic    = errors.New("not an mcap file")
)

const (
	TokenMessage TokenType = iota
	TokenChannelInfo
	TokenFooter
	TokenHeader
	TokenAttachment
	TokenAttachmentIndex
	TokenChunkIndex
	TokenStatistics
	TokenChunk
	TokenMessageIndex
	TokenMetadata
	TokenMetadataIndex
	TokenSummaryOffset
)

type TokenType int

type Token struct {
	TokenType TokenType
	ByteCount int64
	Reader    io.Reader
}

func (t TokenType) String() string {
	switch t {
	case TokenMessage:
		return "message"
	case TokenChannelInfo:
		return "channel info"
	case TokenFooter:
		return "footer"
	case TokenHeader:
		return "header"
	case TokenAttachment:
		return "attachment"
	case TokenAttachmentIndex:
		return "attachment index"
	case TokenChunk:
		return "chunk"
	case TokenChunkIndex:
		return "chunk index"
	case TokenStatistics:
		return "statistics"
	case TokenMessageIndex:
		return "message index"
	case TokenMetadata:
		return "metadata"
	case TokenMetadataIndex:
		return "metadata index"
	case TokenSummaryOffset:
		return "summary offset"
	default:
		return "unknown"
	}
}

func (t Token) String() string {
	return t.TokenType.String()
}

func (t Token) bytes() []byte {
	data := make([]byte, t.ByteCount)
	_, _ = io.ReadFull(t.Reader, data) // TODO
	return data
}

type decoders struct {
	lz4  *lz4.Reader
	zstd *zstd.Decoder
	none *bytes.Reader
}

type Lexer struct {
	basereader io.Reader
	reader     io.Reader
	emitChunks bool

	decoders    decoders
	inChunk     bool
	buf         []byte
	validateCRC bool
}

func validateMagic(r io.Reader) error {
	magic := make([]byte, len(Magic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return ErrBadMagic
	}
	if !bytes.Equal(magic, Magic) {
		return ErrBadMagic
	}
	return nil
}

func (l *Lexer) setNoneDecoder(buf []byte) {
	if l.decoders.none == nil {
		l.decoders.none = bytes.NewReader(buf)
	} else {
		l.decoders.none.Reset(buf)
	}
	l.reader = l.decoders.none
}

func (l *Lexer) setLZ4Decoder(r io.Reader) {
	if l.decoders.lz4 == nil {
		l.decoders.lz4 = lz4.NewReader(r)
	} else {
		l.decoders.lz4.Reset(r)
	}
	l.reader = l.decoders.lz4
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
	l.reader = l.decoders.zstd
	return nil
}

func loadChunk(l *Lexer, recordSize int64) error {
	if l.inChunk {
		return ErrNestedChunk
	}
	_, err := io.ReadFull(l.reader, l.buf[:8+8+8+4+4])
	if err != nil {
		return err
	}

	// the reader does not care about the start, end, or uncompressed size, or
	// they would be using emitChunks.

	// Skip the uncompressed size; the lexer will read messages out of the
	// reader incrementally.
	_, offset, err := getUint64(l.buf, 0) // start
	if err != nil {
		return fmt.Errorf("failed to read start: %w", err)
	}
	_, offset, err = getUint64(l.buf, offset) // end
	if err != nil {
		return fmt.Errorf("failed to read end: %w", err)
	}
	_, offset, err = getUint64(l.buf, offset) // uncompressed size
	if err != nil {
		return fmt.Errorf("failed to read uncompressed size: %w", err)
	}
	uncompressedCRC, offset, err := getUint32(l.buf, offset)
	if err != nil {
		return fmt.Errorf("failed to read uncompressed CRC: %w", err)
	}
	compressionLen, offset, err := getUint32(l.buf, offset)
	if err != nil {
		return fmt.Errorf("failed to read compression length: %w", err)
	}

	compression := make([]byte, compressionLen)
	_, err = io.ReadFull(l.reader, compression)
	if err != nil {
		return err
	}

	// remaining bytes in the record are the chunk data
	lr := io.LimitReader(l.reader, recordSize-int64(offset+len(compression)))
	switch CompressionFormat(compression) {
	case CompressionNone:
		l.reader = lr
	case CompressionLZ4:
		l.setLZ4Decoder(lr)
	case CompressionZSTD:
		err = l.setZSTDDecoder(lr)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported compression: %s", string(compression))
	}

	// if we are validating the CRC, we need to fully decompress the chunk right
	// here, then rewrap the decompressed data in a compatible reader after
	// validation. If we are not validating CRCs, we can use incremental
	// decompression for the chunk's data, which may be beneficial to streaming
	// readers.
	if l.validateCRC {
		uncompressed, err := io.ReadAll(l.reader)
		if err != nil {
			return err
		}
		crc := crc32.ChecksumIEEE(uncompressed)
		if crc != uncompressedCRC {
			return fmt.Errorf("invalid CRC: %x != %x", crc, uncompressedCRC)
		}
		l.setNoneDecoder(uncompressed)
	}
	l.inChunk = true
	return nil
}

func (l *Lexer) Next() (Token, error) {
	for {
		_, err := io.ReadFull(l.reader, l.buf[:9])
		if err != nil {
			unexpectedEOF := errors.Is(err, io.ErrUnexpectedEOF)
			eof := errors.Is(err, io.EOF)
			if l.inChunk && (eof || unexpectedEOF) {
				l.inChunk = false
				l.reader = l.basereader
				continue
			}
			if unexpectedEOF || eof {
				return Token{}, io.EOF
			}
			return Token{}, err
		}
		opcode := OpCode(l.buf[0])
		recordLen := int64(binary.LittleEndian.Uint64(l.buf[1:9]))
		switch opcode {
		case OpHeader:
			return Token{TokenHeader, recordLen, l.reader}, nil
		case OpChannelInfo:
			return Token{TokenChannelInfo, recordLen, l.reader}, nil
		case OpFooter:
			return Token{TokenFooter, recordLen, l.reader}, nil
		case OpMessage:
			return Token{TokenMessage, recordLen, l.reader}, nil
		case OpAttachment:
			return Token{TokenAttachment, recordLen, l.reader}, nil
		case OpAttachmentIndex:
			return Token{TokenAttachmentIndex, recordLen, l.reader}, nil
		case OpChunkIndex:
			if !l.emitChunks {
				_, err := io.CopyN(io.Discard, l.reader, recordLen)
				if err != nil {
					return Token{}, err
				}
				continue
			}
			return Token{TokenChunkIndex, recordLen, l.reader}, nil
		case OpStatistics:
			return Token{TokenStatistics, recordLen, l.reader}, nil
		case OpMessageIndex:
			if !l.emitChunks {
				_, err := io.CopyN(io.Discard, l.reader, recordLen)
				if err != nil {
					return Token{}, err
				}
				continue
			}
			return Token{TokenMessageIndex, recordLen, l.reader}, nil
		case OpChunk:
			if !l.emitChunks {
				err := loadChunk(l, recordLen)
				if err != nil {
					return Token{}, err
				}
				continue
			}
			return Token{TokenChunk, recordLen, l.reader}, nil
		case OpMetadata:
			return Token{TokenMetadata, recordLen, l.reader}, nil
		case OpMetadataIndex:
			return Token{TokenMetadata, recordLen, l.reader}, nil
		case OpSummaryOffset:
			return Token{TokenSummaryOffset, recordLen, l.reader}, nil
		case OpInvalidZero:
			return Token{}, fmt.Errorf("invalid zero opcode")
		default:
			continue // skip unrecognized opcodes
		}
	}
}

type LexOpts struct {
	SkipMagic   bool
	ValidateCRC bool
	EmitChunks  bool
}

func NewLexer(r io.Reader, opts ...*LexOpts) (*Lexer, error) {
	var validateCRC, emitChunks, skipMagic bool
	if len(opts) > 0 {
		validateCRC = opts[0].ValidateCRC
		emitChunks = opts[0].EmitChunks
		skipMagic = opts[0].SkipMagic
	}

	if !skipMagic {
		err := validateMagic(r)
		if err != nil {
			return nil, err
		}
	}
	return &Lexer{
		basereader:  r,
		reader:      r,
		buf:         make([]byte, 32),
		validateCRC: validateCRC,
		emitChunks:  emitChunks,
	}, nil
}
