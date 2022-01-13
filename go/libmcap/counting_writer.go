package libmcap

import (
	"hash"
	"hash/crc32"
	"io"
)

type CountingCRCWriter struct {
	w          ResettableWriteCloser
	size       int64
	crc        hash.Hash32
	computeCRC bool
}

func (c *CountingCRCWriter) Reset(w io.Writer) {
	c.w.Reset(w)
}

func (c *CountingCRCWriter) ResetCRC() {
	c.crc = crc32.NewIEEE()
}

func (c *CountingCRCWriter) ResetSize() {
	c.size = 0
}

func (c *CountingCRCWriter) CRC() uint32 {
	return c.crc.Sum32()
}

func (c *CountingCRCWriter) Size() int64 {
	return c.size
}

func (c *CountingCRCWriter) Close() error {
	return c.w.Close()
}

func (c *CountingCRCWriter) Write(p []byte) (int, error) {
	c.size += int64(len(p))
	if c.computeCRC {
		_, _ = c.crc.Write(p)
	}
	return c.w.Write(p)
}

func NewCountingCRCWriter(w ResettableWriteCloser, computeCRC bool) *CountingCRCWriter {
	return &CountingCRCWriter{
		w:          w,
		crc:        crc32.NewIEEE(),
		computeCRC: computeCRC,
	}
}
