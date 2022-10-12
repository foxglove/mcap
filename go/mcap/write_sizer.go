package mcap

import (
	"io"
)

type writeSizer struct {
	crc  *crcWriter
	w    io.Writer
	size uint64
}

func (w *writeSizer) Write(p []byte) (int, error) {
	w.size += uint64(len(p))
	if w.crc != nil {
		return w.crc.Write(p)
	}
	return w.w.Write(p)
}

func newWriteSizer(w io.Writer, calculateCRC bool) *writeSizer {
	if calculateCRC {
		return &writeSizer{
			crc: newCRCWriter(w),
		}
	}
	return &writeSizer{
		w: w,
	}
}

func (w *writeSizer) Size() uint64 {
	return w.size
}

func (w *writeSizer) Checksum() uint32 {
	if w.crc != nil {
		return w.crc.Checksum()
	}
	return 0
}

func (w *writeSizer) ResetCRC() {
	if w.crc != nil {
		w.crc.Reset()
	}
}
