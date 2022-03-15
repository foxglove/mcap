package mcap

import (
	"hash"
	"hash/crc32"
	"io"
)

type countingCRCWriter struct {
	w          resettableWriteCloser
	size       int64
	crc        hash.Hash32
	computeCRC bool
}

func (c *countingCRCWriter) Reset(w io.Writer) {
	c.w.Reset(w)
}

func (c *countingCRCWriter) ResetCRC() {
	c.crc.Reset()
}

func (c *countingCRCWriter) ResetSize() {
	c.size = 0
}

func (c *countingCRCWriter) CRC() uint32 {
	return c.crc.Sum32()
}

func (c *countingCRCWriter) Size() int64 {
	return c.size
}

func (c *countingCRCWriter) Close() error {
	return c.w.Close()
}

func (c *countingCRCWriter) Write(p []byte) (int, error) {
	c.size += int64(len(p))
	if c.computeCRC {
		_, _ = c.crc.Write(p)
	}
	return c.w.Write(p)
}

func newCountingCRCWriter(w resettableWriteCloser, computeCRC bool) *countingCRCWriter {
	return &countingCRCWriter{
		w:          w,
		crc:        crc32.NewIEEE(),
		computeCRC: computeCRC,
	}
}
