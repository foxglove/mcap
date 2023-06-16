package utils

import (
	"hash/crc32"
	"io"
)

type checksummingWriteCounter struct {
	w     io.Writer
	count int64
	crc   uint32
}

func (cw *checksummingWriteCounter) Write(p []byte) (n int, err error) {
	n, err = cw.w.Write(p)
	cw.count += int64(n)
	cw.crc = crc32.Update(cw.crc, crc32.IEEETable, p)
	return n, err
}

func (cw *checksummingWriteCounter) Count() int64 {
	return cw.count
}

func (cw *checksummingWriteCounter) CRC() uint32 {
	return cw.crc
}

func newChecksummingWriteCounter(w io.Writer, initialCRC uint32) *checksummingWriteCounter {
	return &checksummingWriteCounter{
		w:   w,
		crc: initialCRC,
	}
}
