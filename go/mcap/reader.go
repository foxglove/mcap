package mcap

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

func readPrefixedString(data []byte, offset int) (s string, newoffset int, err error) {
	if len(data[offset:]) < 4 {
		return "", 0, io.ErrShortBuffer
	}
	length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	if len(data[offset+4:]) < length {
		return "", 0, io.ErrShortBuffer
	}
	return string(data[offset+4 : offset+length+4]), offset + 4 + length, nil
}

func readPrefixedBytes(data []byte, offset int) (s []byte, newoffset int, err error) {
	if len(data[offset:]) < 4 {
		return nil, 0, io.ErrShortBuffer
	}
	length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	if len(data[offset+4:]) < length {
		return nil, 0, io.ErrShortBuffer
	}
	return data[offset+4 : offset+length+4], offset + 4 + length, nil
}

func readPrefixedMap(data []byte, offset int) (result map[string]string, newoffset int, err error) {
	var key, value string
	var inset int
	m := make(map[string]string)
	maplen, offset, err := getUint32(data, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read map length: %w", err)
	}
	for uint32(offset+inset) < uint32(offset)+maplen {
		key, inset, err = readPrefixedString(data[offset:], inset)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read map key: %w", err)
		}
		value, inset, err = readPrefixedString(data[offset:], inset)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read map value: %w", err)
		}
		m[key] = value
	}
	return m, offset + inset, nil
}

type Reader struct {
	l        *Lexer
	r        io.Reader
	rs       io.ReadSeeker
	channels map[uint16]*Channel
}

type MessageIterator interface {
	Next([]byte) (*Schema, *Channel, *Message, error)
}

func (r *Reader) unindexedIterator(topics []string, start uint64, end uint64) *unindexedMessageIterator {
	topicMap := make(map[string]bool)
	for _, topic := range topics {
		topicMap[topic] = true
	}
	r.l.emitChunks = false
	return &unindexedMessageIterator{
		lexer:    r.l,
		channels: make(map[uint16]*Channel),
		schemas:  make(map[uint16]*Schema),
		topics:   topicMap,
		start:    start,
		end:      end,
	}
}

func (r *Reader) indexedMessageIterator(topics []string, start uint64, end uint64) *indexedMessageIterator {
	topicMap := make(map[string]bool)
	for _, topic := range topics {
		topicMap[topic] = true
	}
	r.l.emitChunks = true
	return &indexedMessageIterator{
		lexer:    r.l,
		rs:       r.rs,
		channels: make(map[uint16]*Channel),
		schemas:  make(map[uint16]*Schema),
		topics:   topicMap,
		start:    start,
		end:      end,
	}
}

func (r *Reader) Messages(
	start int64,
	end int64,
	topics []string,
	useIndex bool,
) (MessageIterator, error) {
	if useIndex {
		if rs, ok := r.r.(io.ReadSeeker); ok {
			r.rs = rs
		} else {
			return nil, fmt.Errorf("indexed reader requires a seekable reader")
		}
		return r.indexedMessageIterator(topics, uint64(start), uint64(end)), nil
	}
	return r.unindexedIterator(topics, uint64(start), uint64(end)), nil
}

func (r *Reader) readHeader() (*Header, error) {
	_, err := r.rs.Seek(8, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to header: %w", err)
	}
	buf := make([]byte, 9)
	_, err = io.ReadFull(r.rs, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read header length: %w", err)
	}
	if opcode := buf[0]; opcode != byte(OpHeader) {
		return nil, fmt.Errorf("unexpected opcode %d in header", opcode)
	}
	buf = make([]byte, binary.LittleEndian.Uint64(buf[1:]))
	_, err = io.ReadFull(r.rs, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	return ParseHeader(buf)
}

func (r *Reader) Info() (*Info, error) {
	header, err := r.readHeader()
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	it := r.indexedMessageIterator(nil, 0, math.MaxUint64)
	err = it.parseSummarySection()
	if err != nil {
		return nil, err
	}

	return &Info{
		Statistics:        it.statistics,
		Channels:          it.channels,
		ChunkIndexes:      it.chunkIndexes,
		AttachmentIndexes: it.attachmentIndexes,
		Schemas:           it.schemas,
		Header:            header,
	}, nil
}

func NewReader(r io.Reader) (*Reader, error) {
	var rs io.ReadSeeker
	if readseeker, ok := r.(io.ReadSeeker); ok {
		rs = readseeker
	}
	lexer, err := NewLexer(r, &LexerOptions{
		EmitChunks: true,
	})
	if err != nil {
		return nil, err
	}
	return &Reader{
		l:        lexer,
		r:        r,
		rs:       rs,
		channels: make(map[uint16]*Channel),
	}, nil
}
