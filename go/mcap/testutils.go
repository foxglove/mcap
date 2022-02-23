package mcap

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
)

func encodedUint16(x uint16) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, x)
	return buf
}

func encodedUint32(x uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, x)
	return buf
}

func encodedUint64(x uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, x)
	return buf
}

func prefixedString(s string) []byte {
	buf := make([]byte, len(s)+4)
	binary.LittleEndian.PutUint32(buf, uint32(len(s)))
	copy(buf[4:], s)
	return buf
}

func prefixedBytes(s []byte) []byte {
	buf := make([]byte, len(s)+4)
	binary.LittleEndian.PutUint32(buf, uint32(len(s)))
	copy(buf[4:], s)
	return buf
}

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
	buf[0] = byte(OpChannel)
	return buf
}

func message() []byte {
	buf := make([]byte, 9)
	buf[0] = byte(OpMessage)
	return buf
}

func chunk(t *testing.T, compression CompressionFormat, includeCRC bool, records ...[]byte) []byte {
	data := flatten(records...)
	buf := &bytes.Buffer{}
	switch compression {
	case CompressionZSTD:
		w, err := zstd.NewWriter(buf)
		if err != nil {
			t.Errorf("failed to create zstd writer: %s", err)
		}
		_, err = io.Copy(w, bytes.NewReader(data))
		assert.Nil(t, err)
		w.Close()
	case CompressionLZ4:
		w := lz4.NewWriter(buf)
		_, err := io.Copy(w, bytes.NewReader(data))
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
	msglen := uint64(8 + 8 + 8 + 4 + 4 + compressionLen + 8 + compressedLen)
	record := make([]byte, msglen+9)
	offset, err := putByte(record, byte(OpChunk))
	assert.Nil(t, err)
	offset += putUint64(record[offset:], msglen)

	offset += putUint64(record[offset:], 0)   // start
	offset += putUint64(record[offset:], 1e9) // end
	offset += putUint64(record[offset:], uint64(uncompressedLen))
	var crc uint32
	if includeCRC {
		sum := crc32.NewIEEE()
		_, _ = sum.Write(data)
		crc = sum.Sum32()
	} else {
		crc = 0
	}
	offset += putUint32(record[offset:], crc)
	offset += putPrefixedString(record[offset:], string(compression))
	offset += putUint64(record[offset:], uint64(buf.Len()))
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
