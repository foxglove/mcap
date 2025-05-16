package utils

import (
	"hash"
	"hash/crc32"
	"io"
)

type crcReader struct {
	r   io.Reader
	crc hash.Hash32
}

func (r *crcReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		_, _ = r.crc.Write(p[:n])
	}
	return n, err
}

func (r *crcReader) Checksum() uint32 {
	return r.crc.Sum32()
}

func (r *crcReader) Reset() {
	r.crc = crc32.NewIEEE()
}

func newCRCReader(r io.Reader) *crcReader {
	return &crcReader{
		r:   r,
		crc: crc32.NewIEEE(),
	}
}

type ChecksummingReaderCounter struct {
	r     io.Reader
	count int64
}

func (cw *ChecksummingReaderCounter) Read(p []byte) (n int, err error) {
	n, err = cw.r.Read(p)
	cw.count += int64(n)
	return n, err
}

func (cw *ChecksummingReaderCounter) Count() int64 {
	return cw.count
}

func (cw *ChecksummingReaderCounter) Checksum() uint32 {
	if crcReader, ok := cw.r.(*crcReader); ok {
		return crcReader.Checksum()
	}
	return 0
}

func (cw *ChecksummingReaderCounter) ResetCRC() {
	if crcReader, ok := cw.r.(*crcReader); ok {
		crcReader.Reset()
	}
}

func NewChecksummingReaderCounter(w io.Reader, calculateCRC bool) *ChecksummingReaderCounter {
	if calculateCRC {
		return &ChecksummingReaderCounter{
			r: newCRCReader(w),
		}
	}
	return &ChecksummingReaderCounter{
		r: w,
	}
}
