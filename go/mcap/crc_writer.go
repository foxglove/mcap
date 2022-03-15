package mcap

import (
	"hash"
	"hash/crc32"
	"io"
)

type crcWriter struct {
	w   io.Writer
	crc hash.Hash32
}

func (w *crcWriter) Write(p []byte) (int, error) {
	_, _ = w.crc.Write(p)
	return w.w.Write(p)
}

func (w *crcWriter) Checksum() uint32 {
	return w.crc.Sum32()
}

func (w *crcWriter) Reset() {
	w.crc = crc32.NewIEEE()
}

func newCRCWriter(w io.Writer) *crcWriter {
	return &crcWriter{
		w:   w,
		crc: crc32.NewIEEE(),
	}
}
