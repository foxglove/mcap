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
	case TokenError:
		return "error"
	case TokenEOF:
		return "eof"
	}
	return "unknown"
}

func (t Token) String() string {
	switch t.TokenType {
	case TokenError:
		return fmt.Sprintf("error: %s", string(t.bytes()))
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
	TokenEOF
	TokenError
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

type stateFn func(*lexer) stateFn

type decoders struct {
	lz4  *lz4.Reader
	zstd *zstd.Decoder
	none *bytes.Reader
}

type lexer struct {
	state       stateFn
	basereader  io.Reader
	chunkreader io.Reader
	reader      io.Reader
	chunkReader ResettableWriteCloser
	tokens      chan Token
	emitChunks  bool

	compressedChunk []byte
	chunk           []byte
	skipbuf         []byte
	decoders        decoders
	inChunk         bool
	buf             []byte
	validateCRC     bool
}

func (l *lexer) SetLexNext() {
	l.state = lexNext
}

func (l *lexer) Next() Token {
	if l.state == nil {
		return Token{TokenEOF, 0, bytes.NewReader(nil)}
	}
	for {
		select {
		case token := <-l.tokens:
			return token
		default:
			l.state = l.state(l)
		}
	}
}

func (l *lexer) emit(t TokenType, n int64, data io.Reader) {
	l.tokens <- Token{t, n, data}
}

func (l *lexer) error(err error) stateFn {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		l.emit(TokenEOF, 0, bytes.NewReader(nil))
	} else {
		l.emit(TokenError, int64(len(err.Error())), bytes.NewReader([]byte(err.Error())))
	}
	return nil
}

func lexMagic(l *lexer) stateFn {
	magic := make([]byte, len(Magic))
	_, err := l.reader.Read(magic)
	if err != nil {
		return l.error(err)
	}
	if !bytes.Equal(magic, Magic) {
		return l.error(ErrBadMagic)
	}
	return lexNext
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

func skip(l *lexer, n uint64) stateFn {
	if n > uint64(len(l.skipbuf)) {
		l.skipbuf = make([]byte, 2*n)
	}
	_, err := l.reader.Read(l.skipbuf[:n])
	if err != nil {
		return l.error(err)
	}
	return lexNext
}

func lexChunk(l *lexer, recordSize uint64) stateFn {
	if l.inChunk {
		return l.error(ErrNestedChunk)
	}
	_, err := l.reader.Read(l.buf[:8+4+4])
	if err != nil {
		return l.error(err)
	}
	// Skip the uncompressed size; the lexer will read messages out of the
	// reader incrementally.
	_ = binary.LittleEndian.Uint64(l.buf[:8])
	uncompressedCRC := binary.LittleEndian.Uint32(l.buf[8:12])
	compressionLen := binary.LittleEndian.Uint32(l.buf[12:16])
	_, err = l.reader.Read(l.buf[:compressionLen])
	if err != nil {
		return l.error(err)
	}
	compression := l.buf[:compressionLen]
	// will eof at the end of the chunk
	lr := io.LimitReader(l.reader, int64(recordSize-16-uint64(compressionLen)))
	switch CompressionFormat(compression) {
	case CompressionNone:
		l.reader = lr
	case CompressionLZ4:
		l.setLZ4Decoder(lr)
	case CompressionZSTD:
		err = l.setZSTDDecoder(lr)
		if err != nil {
			return l.error(err)
		}
	default:
		return l.error(fmt.Errorf("unsupported compression: %s", string(compression)))
	}

	// if we are validating the CRC, we need to fully decompress the chunk right
	// here, then rewrap the decompressed data in a compatible reader after
	// validation. If we are not validating CRCs, we can use incremental
	// decompression for the chunk's data, which may be beneficial to streaming
	// readers.
	if l.validateCRC {
		uncompressed, err := io.ReadAll(l.reader)
		if err != nil {
			return l.error(err)
		}
		crc := crc32.ChecksumIEEE(uncompressed)
		if crc != uncompressedCRC {
			return l.error(fmt.Errorf("invalid CRC: %x != %x", crc, uncompressedCRC))
		}
		l.setNoneDecoder(uncompressed)
	}
	l.inChunk = true
	return lexNext
}

func lexNext(l *lexer) stateFn {
	_, err := io.ReadFull(l.reader, l.buf[:9])
	if err != nil {
		if l.inChunk && (errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)) { // todo what's going on here
			l.inChunk = false
			l.reader = l.basereader
			return lexNext
		}
		return l.error(err)
	}
	opcode := OpCode(l.buf[0])
	recordLen := binary.LittleEndian.Uint64(l.buf[1:9])
	switch opcode {
	case OpHeader:
		l.emit(TokenHeader, int64(recordLen), l.reader)
	case OpChannelInfo:
		l.emit(TokenChannelInfo, int64(recordLen), l.reader)
	case OpFooter:
		l.emit(TokenFooter, int64(recordLen), l.reader)
		return lexMagic
	case OpMessage:
		l.emit(TokenMessage, int64(recordLen), l.reader)
	case OpAttachment:
		l.emit(TokenAttachment, int64(recordLen), l.reader)
	case OpAttachmentIndex:
		l.emit(TokenAttachmentIndex, int64(recordLen), l.reader)
	case OpChunkIndex:
		if !l.emitChunks {
			return skip(l, recordLen)
		}
		l.emit(TokenChunkIndex, int64(recordLen), l.reader)
	case OpStatistics:
		l.emit(TokenStatistics, int64(recordLen), l.reader)
	case OpMessageIndex:
		if !l.emitChunks {
			return skip(l, recordLen)
		}
		l.emit(TokenMessageIndex, int64(recordLen), l.reader)
	case OpChunk:
		if !l.emitChunks {
			return lexChunk(l, recordLen)
		}
		l.emit(TokenChunk, int64(recordLen), l.reader)
	default:
		return skip(l, recordLen)
	}
	return lexNext
}

type lexOpts struct {
	validateCRC bool
	emitChunks  bool
}

func NewLexer(r io.Reader, opts ...*lexOpts) *lexer {
	var validateCRC, emitChunks bool
	if len(opts) > 0 {
		validateCRC = opts[0].validateCRC
		emitChunks = opts[0].emitChunks
	}
	return &lexer{
		basereader:  r,
		reader:      r,
		tokens:      make(chan Token, 1), // why
		buf:         make([]byte, 32),
		state:       lexMagic,
		validateCRC: validateCRC,
		emitChunks:  emitChunks,
	}
}
