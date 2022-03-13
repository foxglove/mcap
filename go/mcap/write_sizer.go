package mcap

import (
	"hash/crc32"
	"io"
)

type writeSizer struct {
	w    *crcWriter
	size uint64
}

func (w *writeSizer) Write(p []byte) (int, error) {
	w.size += uint64(len(p))
	return w.w.Write(p)
}

func newWriteSizer(w io.Writer) *writeSizer {
	return &writeSizer{
		w: newCRCWriter(w),
	}
}

func (w *writeSizer) Size() uint64 {
	return w.size
}

func (w *writeSizer) Checksum() uint32 {
	return w.w.Checksum()
}

func (w *writeSizer) ResetCRC() {
	w.w.crc = crc32.NewIEEE()
}
