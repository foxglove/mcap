package libmcap

import (
	"hash"
	"hash/crc32"
	"io"
)

type CRCWriter struct {
	w   io.Writer
	crc hash.Hash32
}

func (w *CRCWriter) Write(p []byte) (int, error) {
	_, _ = w.crc.Write(p)
	return w.w.Write(p)
}

func (w *CRCWriter) Checksum() uint32 {
	return w.crc.Sum32()
}

func (w *CRCWriter) Reset() {
	w.crc = crc32.NewIEEE()
}

func NewCRCWriter(w io.Writer) *CRCWriter {
	return &CRCWriter{
		w:   w,
		crc: crc32.NewIEEE(),
	}
}
