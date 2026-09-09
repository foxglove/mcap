package mcap

import (
	"hash"
	"hash/crc32"
	"io"
)

// Bytes are hashed in batches of at least this size (except when the CRC is
// read), so the hasher's hardware-accelerated fast path gets bulk input even
// when the writer receives many small writes. Record serialization emits a
// 9-byte record header plus the record body per record; hashing at that
// granularity keeps hash/crc32 well below its bulk throughput.
const crcHashBatchSize = 32 * 1024

type countingCRCWriter struct {
	w          ResettableWriteCloser
	size       int64
	crc        hash.Hash32
	computeCRC bool
	// Bytes already written to `w` but not yet folded into `crc`. Hash-only
	// batching: I/O passes through immediately, so ordering and flush
	// semantics are unaffected. Every read of the CRC drains these first.
	pending []byte
}

func (c *countingCRCWriter) drainPending() {
	if len(c.pending) > 0 {
		_, _ = c.crc.Write(c.pending)
		c.pending = c.pending[:0]
	}
}

func (c *countingCRCWriter) Reset(w io.Writer) {
	c.w.Reset(w)
}

func (c *countingCRCWriter) ResetCRC() {
	c.pending = c.pending[:0]
	c.crc.Reset()
}

func (c *countingCRCWriter) ResetSize() {
	c.size = 0
}

func (c *countingCRCWriter) CRC() uint32 {
	c.drainPending()
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
		if len(p) >= crcHashBatchSize {
			// Large writes are hashed directly (after any pending bytes, to
			// keep the hashed stream in write order), skipping the copy.
			c.drainPending()
			_, _ = c.crc.Write(p)
		} else {
			c.pending = append(c.pending, p...)
			if len(c.pending) >= crcHashBatchSize {
				c.drainPending()
			}
		}
	}
	return c.w.Write(p)
}

func newCountingCRCWriter(w ResettableWriteCloser, computeCRC bool) *countingCRCWriter {
	return &countingCRCWriter{
		w:          w,
		crc:        crc32.NewIEEE(),
		computeCRC: computeCRC,
	}
}
