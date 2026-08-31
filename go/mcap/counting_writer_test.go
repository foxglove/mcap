package mcap

import (
	"bytes"
	"hash/crc32"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type resettableBuffer struct {
	*bytes.Buffer
}

func (b resettableBuffer) Reset(io.Writer) {}
func (b resettableBuffer) Close() error    { return nil }

func TestCountingCRCWriterBatchedHashMatchesReference(t *testing.T) {
	data := make([]byte, 1_000_000)
	for i := range data {
		data[i] = byte(i % 251)
	}
	reference := crc32.ChecksumIEEE(data)

	// Mix of tiny field-sized writes, payload-sized writes, and writes larger
	// than the batch size, mirroring real record serialization patterns.
	cases := [][]int{
		{1, 2, 4, 8},
		{9, 100},
		{crcHashBatchSize - 1},
		{crcHashBatchSize},
		{crcHashBatchSize + 1},
		{3, 100, crcHashBatchSize + 7, 9, crcHashBatchSize},
	}
	for _, sizes := range cases {
		buf := resettableBuffer{&bytes.Buffer{}}
		w := newCountingCRCWriter(buf, true)
		offset := 0
		for i := 0; offset < len(data); i++ {
			size := sizes[i%len(sizes)]
			if size > len(data)-offset {
				size = len(data) - offset
			}
			n, err := w.Write(data[offset : offset+size])
			require.NoError(t, err)
			require.Equal(t, size, n)
			offset += size
		}
		assert.Equal(t, int64(len(data)), w.Size())
		// CRC() must account for unhashed pending bytes, and reading it twice
		// must return the same value.
		assert.Equal(t, reference, w.CRC())
		assert.Equal(t, reference, w.CRC())
		assert.Equal(t, data, buf.Bytes())
	}
}

func TestCountingCRCWriterResetCRCClearsPending(t *testing.T) {
	buf := resettableBuffer{&bytes.Buffer{}}
	w := newCountingCRCWriter(buf, true)
	_, err := w.Write([]byte("stale pending bytes"))
	require.NoError(t, err)
	w.ResetCRC()
	payload := []byte("fresh")
	_, err = w.Write(payload)
	require.NoError(t, err)
	assert.Equal(t, crc32.ChecksumIEEE(payload), w.CRC())
}

func TestCountingCRCWriterDisabledAccumulatesNothing(t *testing.T) {
	buf := resettableBuffer{&bytes.Buffer{}}
	w := newCountingCRCWriter(buf, false)
	_, err := w.Write([]byte{1, 2, 3})
	require.NoError(t, err)
	assert.Equal(t, uint32(0), w.CRC())
	assert.Empty(t, w.pending)
	assert.Equal(t, []byte{1, 2, 3}, buf.Bytes())
}
