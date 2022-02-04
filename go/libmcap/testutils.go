package libmcap

import (
	"bytes"
	"hash/crc32"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
)

func flatten(slices ...[]byte) []byte {
	var flattened []byte
	for _, s := range slices {
		flattened = append(flattened, s...)
	}
	return flattened
}

func file(records ...[]byte) []byte {
	var file [][]byte
	file = append(file, Magic)
	file = append(file, records...)
	file = append(file, Magic)
	return flatten(file...)
}

func footer() []byte {
	buf := make([]byte, 9)
	buf[0] = byte(OpFooter)
	return buf
}

func header() []byte {
	buf := make([]byte, 9)
	buf[0] = byte(OpHeader)
	return buf
}

func channelInfo() []byte {
	buf := make([]byte, 9)
	buf[0] = byte(OpChannelInfo)
	return buf
}

func message() []byte {
	buf := make([]byte, 9)
	buf[0] = byte(OpMessage)
	return buf
}

func chunk(t *testing.T, compression CompressionFormat, records ...[]byte) []byte {
	data := flatten(records...)
	buf := &bytes.Buffer{}
	switch compression {
	case CompressionLZ4:
		w := lz4.NewWriter(buf)
		_, err := io.Copy(w, bytes.NewReader(data))
		assert.Nil(t, err)
		w.Close()
	case CompressionZSTD:
		w, err := zstd.NewWriter(buf)
		if err != nil {
			t.Errorf("failed to create zstd writer: %s", err)
		}
		_, err = io.Copy(w, bytes.NewReader(data))
		assert.Nil(t, err)
		w.Close()
	case CompressionNone:
		_, err := buf.Write(data)
		assert.Nil(t, err)
	default:
		_, err := buf.Write(data) // unrecognized compression
		assert.Nil(t, err)
	}
	compressionLen := len(compression)
	compressedLen := buf.Len()
	uncompressedLen := len(data)
	msglen := uint64(8 + 8 + 8 + 4 + 4 + compressionLen + compressedLen)
	record := make([]byte, msglen+9)
	offset, err := putByte(record, byte(OpChunk))
	assert.Nil(t, err)
	offset += putUint64(record[offset:], msglen)

	offset += putUint64(record[offset:], 0)   // start
	offset += putUint64(record[offset:], 1e9) // end
	offset += putUint64(record[offset:], uint64(uncompressedLen))
	crc := crc32.NewIEEE()
	_, _ = crc.Write(data)
	offset += putUint32(record[offset:], crc.Sum32())
	offset += putPrefixedString(record[offset:], string(compression))
	_ = copy(record[offset:], buf.Bytes())
	return record
}

func record(op OpCode) []byte {
	buf := make([]byte, 9)
	buf[0] = byte(op)
	return buf
}

func attachment() []byte {
	buf := make([]byte, 9)
	buf[0] = byte(OpAttachment)
	return buf
}
