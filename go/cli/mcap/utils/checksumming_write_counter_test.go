package utils

import (
	"bytes"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChecksummingWriteCounter(t *testing.T) {
	data := []byte("hello, world!")
	fullCRC := crc32.ChecksumIEEE(data)
	initialCRC := crc32.ChecksumIEEE(data[:5])
	buf := &bytes.Buffer{}
	cw := newChecksummingWriteCounter(buf, initialCRC)
	n, err := cw.Write(data[5:])
	require.NoError(t, err)
	assert.Equal(t, len(data[5:]), n, "number of bytes written does not match expected")
	assert.Equal(t, fullCRC, cw.CRC(), "computed CRC does not match expected")
	assert.Equal(t, int64(len(data[5:])), cw.Count(), "count does not match expected")
	assert.Equal(t, data[5:], buf.Bytes(), "data does not match expected")
}
