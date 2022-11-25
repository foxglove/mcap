package mcap

import (
	"bytes"
	"fmt"
	"math"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type ChunkWriter struct {
	compressed        *bytes.Buffer
	compressedWriter  *countingCRCWriter
	compressionFormat CompressionFormat
	MessageIndexes    map[uint16]*MessageIndex

	ChunkStartTime uint64
	ChunkEndTime   uint64
}

func newChunkWriter(compression CompressionFormat, chunkSize int64, includeCRC bool) (*ChunkWriter, error) {
	var compressedWriter *countingCRCWriter
	compressed := &bytes.Buffer{}
	switch compression {
	case CompressionZSTD:
		zw, err := zstd.NewWriter(compressed, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			return nil, err
		}
		compressedWriter = newCountingCRCWriter(zw, includeCRC)
	case CompressionLZ4:
		compressedWriter = newCountingCRCWriter(lz4.NewWriter(compressed), includeCRC)
	case CompressionNone:
		compressedWriter = newCountingCRCWriter(bufCloser{compressed}, includeCRC)
	default:
		return nil, fmt.Errorf("unsupported compression %s", compression)
	}
	return &ChunkWriter{
		compressed:        compressed,
		compressedWriter:  compressedWriter,
		compressionFormat: compression,
		MessageIndexes:    make(map[uint16]*MessageIndex),
		ChunkStartTime:    math.MaxUint64,
		ChunkEndTime:      0,
	}, nil
}

func (cw *ChunkWriter) Write(buf []byte) (int, error) {
	return cw.compressedWriter.Write(buf)
}

func (cw *ChunkWriter) UncompressedLen() int64 {
	return cw.compressedWriter.Size()
}

func (cw *ChunkWriter) CompressedLen() int {
	return cw.compressed.Len()
}

func (cw *ChunkWriter) SerializedLen() int {
	return 8 + 8 + 8 + 4 + 4 + len(cw.compressionFormat) + 8 + cw.CompressedLen()
}

func (cw *ChunkWriter) SerializeTo(buf []byte) (int, error) {
	if len(buf) < cw.SerializedLen() {
		return 0, fmt.Errorf("chunk buffer too small to serialize")
	}
	offset := putUint64(buf, cw.ChunkStartTime)
	offset += putUint64(buf[offset:], cw.ChunkEndTime)
	offset += putUint64(buf[offset:], uint64(cw.UncompressedLen()))
	offset += putUint32(buf[offset:], cw.compressedWriter.CRC())
	offset += putPrefixedString(buf[offset:], string(cw.compressionFormat))
	offset += putUint64(buf[offset:], uint64(cw.CompressedLen()))
	offset += copy(buf[offset:], cw.compressed.Bytes())
	return offset, nil
}

func (cw *ChunkWriter) Close() error {
	return cw.compressedWriter.Close()
}

func (cw *ChunkWriter) Reset() {
	cw.compressed.Reset()
	cw.compressedWriter.Reset(cw.compressed)
	cw.compressedWriter.ResetCRC()
	cw.compressedWriter.ResetSize()
	cw.ChunkStartTime = math.MaxUint64
	cw.ChunkEndTime = 0
}
