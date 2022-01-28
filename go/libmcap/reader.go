package libmcap

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

func readPrefixedString(data []byte, offset int) (string, int, error) {
	if len(data[offset:]) < 4 {
		return "", 0, io.ErrShortBuffer
	}
	length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	if len(data[offset+4:]) < length {
		return "", 0, io.ErrShortBuffer
	}
	return string(data[offset+4 : offset+length+4]), offset + 4 + length, nil
}

func readPrefixedBytes(data []byte, offset int) ([]byte, int, error) {
	if len(data[offset:]) < 4 {
		return nil, 0, io.ErrShortBuffer
	}
	length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	if len(data[offset+4:]) < length {
		return nil, 0, io.ErrShortBuffer
	}
	return data[offset+4 : offset+length+4], offset + 4 + length, nil
}

func readPrefixedMap(data []byte, offset int) (map[string]string, int, error) {
	var err error
	var key, value string
	var inset int
	m := make(map[string]string)
	maplen, offset := getUint32(data, offset)
	for uint32(offset+inset) < uint32(offset)+maplen {
		key, inset, err = readPrefixedString(data[offset:], inset)
		if err != nil {
			return nil, 0, err
		}
		value, inset, err = readPrefixedString(data[offset:], inset)
		if err != nil {
			return nil, 0, err
		}
		m[key] = value
	}
	return m, offset + inset, nil
}

func readMessageIndexEntries(data []byte, offset int) ([]MessageIndexEntry, int, error) {
	entries := make([]MessageIndexEntry, 0)
	entriesByteLength, offset := getUint32(data, offset)
	var start = offset
	for uint32(offset) < uint32(start)+entriesByteLength {
		stamp, offset := getUint64(data, offset)
		value, offset := getUint64(data, offset)
		entries = append(entries, MessageIndexEntry{
			Timestamp: stamp,
			Offset:    value,
		})
	}
	return entries, offset, nil
}

type Reader struct {
	l                 *lexer
	r                 io.Reader
	rs                io.ReadSeeker
	channels          map[uint16]*ChannelInfo
	statistics        *Statistics
	chunkIndexes      []*ChunkIndex
	attachmentIndexes []*AttachmentIndex
}

type MessageIterator interface {
	Next() (*ChannelInfo, *Message, error)
}

func (r *Reader) unindexedIterator(topics []string, start uint64, end uint64) *unindexedMessageIterator {
	topicMap := make(map[string]bool)
	for _, topic := range topics {
		topicMap[topic] = true
	}
	r.l.emitChunks = false
	return &unindexedMessageIterator{
		lexer:    r.l,
		channels: make(map[uint16]*ChannelInfo),
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
		lexer:               r.l,
		rs:                  r.rs,
		channels:            make(map[uint16]*ChannelInfo),
		topics:              topicMap,
		start:               start,
		end:                 end,
		activeChunksetIndex: -1,
		activeChunkIndex:    -1,
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

func (r *Reader) Info() (*Info, error) {
	it := r.indexedMessageIterator(nil, 0, math.MaxUint64)
	err := it.parseIndexSection()
	if err != nil {
		return nil, err
	}
	return &Info{
		Statistics:   it.statistics,
		Channels:     it.channels,
		ChunkIndexes: it.chunkIndexes,
	}, nil
}

func NewReader(r io.Reader) (*Reader, error) {
	var rs io.ReadSeeker
	if readseeker, ok := r.(io.ReadSeeker); ok {
		rs = readseeker
	}
	lexer, err := NewLexer(r, &LexOpts{
		EmitChunks: true,
	})
	if err != nil {
		return nil, err
	}
	return &Reader{
		l:        lexer,
		r:        r,
		rs:       rs,
		channels: make(map[uint16]*ChannelInfo),
	}, nil
}
