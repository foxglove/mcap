package mcap

import (
	"hash"
	"hash/crc32"
	"io"
)

type crcReader struct {
	r          io.Reader
	crc        hash.Hash32
	computeCRC bool
}

func (r *crcReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if r.computeCRC {
		_, _ = r.crc.Write(p[:n])
	}
	return n, err
}

func (r *crcReader) Checksum() uint32 {
	return r.crc.Sum32()
}

func newCRCReader(r io.Reader, computeCRC bool) *crcReader {
	return &crcReader{
		r:          r,
		crc:        crc32.NewIEEE(),
		computeCRC: computeCRC,
	}
}
