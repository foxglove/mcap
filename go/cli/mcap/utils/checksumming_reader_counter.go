package utils

import (
	"hash"
	"hash/crc32"
	"io"
)

type ChecksummingReaderCounter struct {
	r          io.Reader
	count      int64
	crc        hash.Hash32
	computeCRC bool
}

func (cw *ChecksummingReaderCounter) Read(p []byte) (n int, err error) {
	n, err = cw.r.Read(p)
	cw.count += int64(n)
	if cw.computeCRC {
		_, _ = cw.crc.Write(p[:n])
	}
	return n, err
}

func (cw *ChecksummingReaderCounter) Count() int64 {
	return cw.count
}

func (cw *ChecksummingReaderCounter) CRC() uint32 {
	return cw.crc.Sum32()
}

func (cw *ChecksummingReaderCounter) ResetCRC() {
	cw.crc.Reset()
}

func NewChecksummingReaderCounter(w io.Reader, computeCRC bool) *ChecksummingReaderCounter {
	return &ChecksummingReaderCounter{
		r:          w,
		crc:        crc32.NewIEEE(),
		computeCRC: computeCRC,
	}
}
