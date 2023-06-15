package testutils

import (
	"fmt"
	"io"
)

// BufReadWriteSeeker is an io.ReadWriteSeeker backed by memory.
type BufReadWriteSeeker struct {
	buf      []byte
	offset   int64
	length   int64
	capacity int64
}

func (b *BufReadWriteSeeker) Write(p []byte) (n int, err error) {
	if b.offset+int64(len(p)) > b.capacity {
		newBuf := make([]byte, b.capacity*2)
		copy(newBuf, b.buf)
		b.buf = newBuf
		b.capacity = b.capacity * 2
		return b.Write(p)
	}
	if b.offset+int64(len(p)) > b.length {
		b.length = b.offset + int64(len(p))
	}
	n = copy(b.buf[b.offset:], p)
	b.offset += int64(n)
	return n, nil
}

func (b *BufReadWriteSeeker) Read(p []byte) (n int, err error) {
	n = copy(p, b.buf[b.offset:b.length])
	b.offset += int64(n)
	if b.offset > b.length {
		return n, io.ErrUnexpectedEOF
	}
	if b.offset == b.length {
		return n, io.EOF
	}
	return n, nil
}

func (b *BufReadWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		b.offset = offset
	case io.SeekCurrent:
		b.offset += offset
	case io.SeekEnd:
		b.offset = b.length + offset
	}
	if b.offset < 0 {
		return 0, fmt.Errorf("cannot seek before start of file")
	}
	if b.offset > b.length {
		return 0, fmt.Errorf("cannot seek past end of file")
	}
	return b.offset, nil
}

func (b *BufReadWriteSeeker) Bytes() []byte {
	return b.buf[:b.length]
}

func NewBufReadWriteSeeker() *BufReadWriteSeeker {
	return &BufReadWriteSeeker{
		buf:      make([]byte, 1024),
		offset:   0,
		length:   0,
		capacity: 1024,
	}
}
