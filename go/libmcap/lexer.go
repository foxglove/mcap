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

type TokenType int

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
	}
	return "unknown"
}

func (t Token) String() string {
	switch t.TokenType {
	default:
		return t.TokenType.String()
	}
}

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
)

type Token struct {
	TokenType TokenType
	ByteCount int64
	Reader    io.Reader
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

type lexer struct {
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
	_, err := io.ReadFull(r, magic)
	if err != nil {
		return ErrBadMagic
	}
	if !bytes.Equal(magic, Magic) {
		return ErrBadMagic
	}
	return nil
}

func (l *lexer) setNoneDecoder(buf []byte) {
	if l.decoders.none == nil {
		l.decoders.none = bytes.NewReader(buf)
	} else {
		l.decoders.none.Reset(buf)
	}
	l.reader = l.decoders.none
}

func (l *lexer) setLZ4Decoder(r io.Reader) {
	if l.decoders.lz4 == nil {
		l.decoders.lz4 = lz4.NewReader(r)
	} else {
		l.decoders.lz4.Reset(r)
	}
	l.reader = l.decoders.lz4
}

func (l *lexer) setZSTDDecoder(r io.Reader) error {
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

func loadChunk(l *lexer, recordSize int64) error {
	if l.inChunk {
		return ErrNestedChunk
	}
	_, err := io.ReadFull(l.reader, l.buf[:8+4+4])
	if err != nil {
		return err
	}
	// Skip the uncompressed size; the lexer will read messages out of the
	// reader incrementally.
	_ = binary.LittleEndian.Uint64(l.buf[:8])
	uncompressedCRC := binary.LittleEndian.Uint32(l.buf[8:12])
	compressionLen := binary.LittleEndian.Uint32(l.buf[12:16])
	_, err = io.ReadFull(l.reader, l.buf[:compressionLen])
	if err != nil {
		return err
	}
	compression := l.buf[:compressionLen]
	// will eof at the end of the chunk
	lr := io.LimitReader(l.reader, int64(uint64(recordSize)-16-uint64(compressionLen)))
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

func (l *lexer) Next() (Token, error) {
	for {
		_, err := io.ReadFull(l.reader, l.buf[:9])
		if err != nil {
			unexpectedEOF := errors.Is(err, io.ErrUnexpectedEOF)
			eof := errors.Is(err, io.EOF)
			if l.inChunk && eof {
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

func NewLexer(r io.Reader, opts ...*LexOpts) (*lexer, error) {
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
	return &lexer{
		basereader:  r,
		reader:      r,
		buf:         make([]byte, 32),
		validateCRC: validateCRC,
		emitChunks:  emitChunks,
	}, nil
}
