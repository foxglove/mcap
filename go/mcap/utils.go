package mcap

import (
	"encoding/binary"
	"io"
)

func putByte(buf []byte, x byte) (int, error) {
	if len(buf) < 1 {
		return 0, io.ErrShortBuffer
	}
	buf[0] = x
	return 1, nil
}

func getUint16(buf []byte, offset int) (x uint16, newoffset int, err error) {
	if offset > len(buf)-2 {
		return 0, 0, io.ErrShortBuffer
	}
	return binary.LittleEndian.Uint16(buf[offset:]), offset + 2, nil
}

func getUint32(buf []byte, offset int) (x uint32, newoffset int, err error) {
	if offset > len(buf)-4 {
		return 0, 0, io.ErrShortBuffer
	}
	return binary.LittleEndian.Uint32(buf[offset:]), offset + 4, nil
}

func getUint64(buf []byte, offset int) (x uint64, newoffset int, err error) {
	if offset > len(buf)-8 {
		return 0, 0, io.ErrShortBuffer
	}
	return binary.LittleEndian.Uint64(buf[offset:]), offset + 8, nil
}

func putUint16(buf []byte, i uint16) int {
	binary.LittleEndian.PutUint16(buf, i)
	return 2
}

func putUint32(buf []byte, i uint32) int {
	binary.LittleEndian.PutUint32(buf, i)
	return 4
}

func putUint64(buf []byte, i uint64) int {
	binary.LittleEndian.PutUint64(buf, i)
	return 8
}

func putPrefixedString(buf []byte, s string) int {
	offset := putUint32(buf, uint32(len(s)))
	offset += copy(buf[offset:], s)
	return offset
}

func putPrefixedBytes(buf []byte, s []byte) int {
	offset := putUint32(buf, uint32(len(s)))
	offset += copy(buf[offset:], s)
	return offset
}

// ReadIntoOrReplace returns a slice of length `length`, read out of the reader `r`.
// if `buf` is large enough, the returned slice will be sliced out of `buf`. Otherwise,
// `buf` will be replaced by a new, larger buffer, and the returned slice will be sliced
// from that.
func ReadIntoOrReplace(r io.Reader, length int64, buf *[]byte) ([]byte, error) {
	if len(*buf) < int(length) {
		newBuf := make([]byte, length)
		*buf = newBuf
		_, err := io.ReadFull(r, newBuf)
		return newBuf, err
	}
	out := (*buf)[:length]
	_, err := io.ReadFull(r, out)
	return out, err
}
