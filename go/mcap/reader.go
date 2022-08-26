package mcap

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

type ReadOrder int

const (
	ReadOrderFile           ReadOrder = 0
	ReadOrderLogTime                  = 1
	ReadOrderReverseLogTime           = 2
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
	order ReadOrder,
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

type readOptions struct {
	start    int64
	end      int64
	topics   []string
	useIndex bool
	order    ReadOrder
}

func defaultReadOptions() readOptions {
	return readOptions{
		start:    0,
		end:      math.MaxInt64,
		topics:   nil,
		useIndex: true,
		order:    ReadOrderFile,
	}
}

type ReadOpt func(*readOptions) error

func ReadMessagesAfter(start int64) ReadOpt {
	return func(ro *readOptions) error {
		if ro.end < start {
			return fmt.Errorf("end cannot come before start")
		}
		ro.start = start
		return nil
	}
}

func ReadMessagesBefore(end int64) ReadOpt {
	return func(ro *readOptions) error {
		if end < ro.start {
			return fmt.Errorf("end cannot come before start")
		}
		ro.end = end
		return nil
	}
}

func ReadMessagesWithTopics(topics []string) ReadOpt {
	return func(ro *readOptions) error {
		ro.topics = topics
		return nil
	}
}

func ReadMessagesInOrder(order ReadOrder) ReadOpt {
	return func(ro *readOptions) error {
		if !ro.useIndex && order != ReadOrderFile {
			return fmt.Errorf("only file-order reads are supported when not using index")
		}
		ro.order = order
		return nil
	}
}

func ReadMessagesUsingIndex(useIndex bool) ReadOpt {
	return func(ro *readOptions) error {
		if ro.order != ReadOrderFile && !useIndex {
			return fmt.Errorf("only file-order reads are supported when not using index")
		}
		ro.useIndex = useIndex
		return nil
	}
}

func (r *Reader) Messages(
	opts ...ReadOpt,
) (MessageIterator, error) {
	ro := defaultReadOptions()
	for _, opt := range opts {
		err := opt(&ro)
		if err != nil {
			return nil, err
		}
	}
	if ro.useIndex {
		if rs, ok := r.r.(io.ReadSeeker); ok {
			r.rs = rs
		} else {
			return nil, fmt.Errorf("indexed reader requires a seekable reader")
		}
		return r.indexedMessageIterator(ro.topics, uint64(ro.start), uint64(ro.end), ro.order), nil
	}
	return r.unindexedIterator(ro.topics, uint64(ro.start), uint64(ro.end)), nil
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
	it := r.indexedMessageIterator(nil, 0, math.MaxUint64, ReadOrderFile)
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
