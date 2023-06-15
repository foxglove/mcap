package testutils

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufReadWriteSeeker(t *testing.T) {
	t.Run("writing", func(t *testing.T) {
		buf := NewBufReadWriteSeeker()
		data := []byte("hello, world!")
		n, err := buf.Write(data)
		assert.Nil(t, err)
		assert.Equal(t, len(data), n, "number of bytes written does not match expected")
		assert.Equal(t, data, buf.Bytes(), "data does not match expected")
	})

	t.Run("seeking & reading", func(t *testing.T) {
		buf := NewBufReadWriteSeeker()
		data := []byte("hello, world!")
		_, err := buf.Write(data)
		assert.Nil(t, err)

		_, err = buf.Seek(0, io.SeekStart)
		assert.Nil(t, err)

		written, err := io.ReadAll(buf)
		assert.Nil(t, err)
		assert.Equal(t, data, written, "data does not match expected")
	})

	t.Run("overwriting", func(t *testing.T) {
		buf := NewBufReadWriteSeeker()
		data := []byte("hello, world!")
		_, err := buf.Write(data)
		assert.Nil(t, err)

		_, err = buf.Seek(-6, io.SeekCurrent)
		assert.Nil(t, err)

		_, err = buf.Write([]byte("wrold!"))
		assert.Nil(t, err)

		_, err = buf.Seek(0, io.SeekStart)
		assert.Nil(t, err)

		expected := []byte("hello, wrold!")
		assert.Equal(t, expected, buf.Bytes(), "data does not match expected")
	})
}
