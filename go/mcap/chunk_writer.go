package mcap

import (
	"bytes"
)

type ChunkWriter struct {
	uncompressed     *bytes.Buffer
	compressed       *bytes.Buffer
	compressedWriter *countingCRCWriter

	ChunkStartTime uint64
	ChunkEndTime   uint64
}

func newChunkWriter(opts *WriterOptions) ChunkWriter {
}

func (cw *ChunkWriter) Write(buf []byte) (int, error) {
}

func (cw *ChunkWriter) UncompressedLen() int {

}

func (cw *ChunkWriter) CompressedLen() int {

}

func (cw *ChunkWriter) RecordLen() int {

}

func (cw *ChunkWriter) SerializeTo(buf []byte) (int, error) {

}

func (cw *ChunkWriter) Close() error {

}

func (cw *ChunkWriter) Reset() {

}
