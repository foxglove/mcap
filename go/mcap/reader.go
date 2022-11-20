package mcap

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/foxglove/mcap/go/mcap/readopts"
)

func getPrefixedString(data []byte, offset int) (s string, newoffset int, err error) {
	if len(data[offset:]) < 4 {
		return "", 0, io.ErrShortBuffer
	}
	length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	if len(data[offset+4:]) < length {
		return "", 0, io.ErrShortBuffer
	}
	return string(data[offset+4 : offset+length+4]), offset + 4 + length, nil
}

func getPrefixedBytes(data []byte, offset int) (s []byte, newoffset int, err error) {
	if len(data[offset:]) < 4 {
		return nil, 0, io.ErrShortBuffer
	}
	length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	if len(data[offset+4:]) < length {
		return nil, 0, io.ErrShortBuffer
	}
	return data[offset+4 : offset+length+4], offset + 4 + length, nil
}

func getPrefixedMap(data []byte, offset int) (result map[string]string, newoffset int, err error) {
	var key, value string
	var inset int
	m := make(map[string]string)
	maplen, offset, err := getUint32(data, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read map length: %w", err)
	}
	for uint32(offset+inset) < uint32(offset)+maplen {
		key, inset, err = getPrefixedString(data[offset:], inset)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read map key: %w", err)
		}
		value, inset, err = getPrefixedString(data[offset:], inset)
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
	header   *Header
	channels map[uint16]*Channel
}

type MessageIterator interface {
	Next([]byte) (*Schema, *Channel, *Message, error)
}

func Range(it MessageIterator, f func(*Schema, *Channel, *Message) error) error {
	for {
		schema, channel, message, err := it.Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("failed to read record: %w", err)
		}
		err = f(schema, channel, message)
		if err != nil {
			return fmt.Errorf("failed to process record: %w", err)
		}
	}
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

func (r *Reader) indexedMessageIterator(
	topics []string,
	start uint64,
	end uint64,
	order readopts.ReadOrder,
) *indexedMessageIterator {
	topicMap := make(map[string]bool)
	for _, topic := range topics {
		topicMap[topic] = true
	}
	r.l.emitChunks = true
	return &indexedMessageIterator{
		lexer:     r.l,
		rs:        r.rs,
		channels:  make(map[uint16]*Channel),
		schemas:   make(map[uint16]*Schema),
		topics:    topicMap,
		start:     start,
		end:       end,
		indexHeap: rangeIndexHeap{order: order},
	}
}

func (r *Reader) Messages(
	opts ...readopts.ReadOpt,
) (MessageIterator, error) {
	ro := readopts.Default()
	for _, opt := range opts {
		err := opt(&ro)
		if err != nil {
			return nil, err
		}
	}
	if ro.UseIndex {
		if rs, ok := r.r.(io.ReadSeeker); ok {
			r.rs = rs
		} else {
			return nil, fmt.Errorf("indexed reader requires a seekable reader")
		}
		return r.indexedMessageIterator(ro.Topics, uint64(ro.Start), uint64(ro.End), ro.Order), nil
	}
	return r.unindexedIterator(ro.Topics, uint64(ro.Start), uint64(ro.End)), nil
}

// Get the Header record from this MCAP.
func (r *Reader) Header() *Header {
	return r.header
}

func (r *Reader) Info() (*Info, error) {
	it := r.indexedMessageIterator(nil, 0, math.MaxUint64, readopts.FileOrder)
	err := it.parseSummarySection()
	if err != nil {
		return nil, err
	}

	return &Info{
		Statistics:        it.statistics,
		Channels:          it.channels,
		ChunkIndexes:      it.chunkIndexes,
		AttachmentIndexes: it.attachmentIndexes,
		MetadataIndexes:   it.metadataIndexes,
		Schemas:           it.schemas,
		Header:            r.header,
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
	token, headerData, err := lexer.Next(nil)
	if err != nil {
		return nil, fmt.Errorf("could not read MCAP header when opening reader: %w", err)
	}
	if token != TokenHeader {
		return nil, fmt.Errorf("expected first record in MCAP to be a Header, found %v", headerData)
	}
	header, err := ParseHeader(headerData)
	if err != nil {
		return nil, err
	}
	return &Reader{
		l:        lexer,
		r:        r,
		rs:       rs,
		header:   header,
		channels: make(map[uint16]*Channel),
	}, nil
}
