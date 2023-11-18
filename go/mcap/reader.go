package mcap

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

var ErrMetadataNotFound = errors.New("metadata not found")

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

	info *Info
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

func (r *Reader) unindexedIterator(opts ReadOptions) *unindexedMessageIterator {
	topicMap := make(map[string]bool)
	for _, topic := range opts.Topics {
		topicMap[topic] = true
	}
	r.l.emitChunks = false
	return &unindexedMessageIterator{
		lexer:            r.l,
		channels:         make(map[uint16]*Channel),
		schemas:          make(map[uint16]*Schema),
		topics:           topicMap,
		start:            uint64(opts.Start),
		end:              uint64(opts.End),
		metadataCallback: opts.MetadataCallback,
	}
}

func (r *Reader) indexedMessageIterator(
	opts ReadOptions,
) *indexedMessageIterator {
	topicMap := make(map[string]bool)
	for _, topic := range opts.Topics {
		topicMap[topic] = true
	}
	r.l.emitChunks = true
	return &indexedMessageIterator{
		lexer:            r.l,
		rs:               r.rs,
		channels:         make(map[uint16]*Channel),
		schemas:          make(map[uint16]*Schema),
		topics:           topicMap,
		start:            uint64(opts.Start),
		end:              uint64(opts.End),
		indexHeap:        rangeIndexHeap{order: opts.Order},
		metadataCallback: opts.MetadataCallback,
	}
}

func (r *Reader) Messages(
	opts ...ReadOpt,
) (MessageIterator, error) {
	options := ReadOptions{
		Start:    0,
		End:      math.MaxInt64,
		Topics:   nil,
		UseIndex: true,
		Order:    FileOrder,
	}
	for _, opt := range opts {
		err := opt(&options)
		if err != nil {
			return nil, err
		}
	}
	if options.UseIndex {
		if rs, ok := r.r.(io.ReadSeeker); ok {
			r.rs = rs
		} else {
			return nil, fmt.Errorf("indexed reader requires a seekable reader")
		}
		return r.indexedMessageIterator(options), nil
	}
	return r.unindexedIterator(options), nil
}

// Get the Header record from this MCAP.
func (r *Reader) Header() *Header {
	return r.header
}

// Info scans the summary section to form a structure describing characteristics
// of the underlying mcap file.
func (r *Reader) Info() (*Info, error) {
	if r.info != nil {
		return r.info, nil
	}
	if r.rs == nil {
		return nil, fmt.Errorf("cannot get info from non-seekable reader")
	}
	it := r.indexedMessageIterator(ReadOptions{
		UseIndex: true,
	})
	err := it.parseSummarySection()
	if err != nil {
		return nil, err
	}
	info := &Info{
		Statistics:        it.statistics,
		Channels:          it.channels,
		ChunkIndexes:      it.chunkIndexes,
		AttachmentIndexes: it.attachmentIndexes,
		MetadataIndexes:   it.metadataIndexes,
		Schemas:           it.schemas,
		Footer:            it.footer,
		Header:            r.header,
	}
	r.info = info
	return info, nil
}

// GetAttachmentReader returns an attachment reader located at the specific offset.
// The reader must be consumed before the base reader is used again.
func (r *Reader) GetAttachmentReader(offset uint64) (*AttachmentReader, error) {
	_, err := r.rs.Seek(int64(offset+9), io.SeekStart)
	if err != nil {
		return nil, err
	}
	ar, err := parseAttachmentReader(r.rs, true)
	if err != nil {
		return nil, err
	}
	return ar, nil
}

func (r *Reader) GetMetadata(offset uint64) (*Metadata, error) {
	_, err := r.rs.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	token, data, err := r.l.Next(nil)
	if err != nil {
		return nil, err
	}
	if token != TokenMetadata {
		return nil, fmt.Errorf("expected metadata record, found %v", data)
	}
	metadata, err := ParseMetadata(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata record: %w", err)
	}
	return metadata, nil
}

// Close the reader.
func (r *Reader) Close() {
	r.l.Close()
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
	defer lexer.Close()
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
