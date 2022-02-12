package libmcap

import (
	"hash/crc32"
	"io"
)

type WriteSizer struct {
	w    *CRCWriter
	size uint64
}

func (w *WriteSizer) Write(p []byte) (int, error) {
	w.size += uint64(len(p))
	return w.w.Write(p)
}

func NewWriteSizer(w io.Writer) *WriteSizer {
	return &WriteSizer{
		w: NewCRCWriter(w),
	}
}

func (w *WriteSizer) Size() uint64 {
	return w.size
}

func (w *WriteSizer) Checksum() uint32 {
	return w.w.Checksum()
}

func (w *WriteSizer) Reset() {
	w.w.crc = crc32.NewIEEE()
}
