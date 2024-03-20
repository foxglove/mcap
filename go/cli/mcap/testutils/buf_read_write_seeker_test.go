package testutils

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufReadWriteSeeker(t *testing.T) {
	t.Run("writing", func(t *testing.T) {
		buf := NewBufReadWriteSeeker()
		data := []byte("hello, world!")
		n, err := buf.Write(data)
		require.NoError(t, err)
		require.Equal(t, len(data), n, "number of bytes written does not match expected")
		require.Equal(t, data, buf.Bytes(), "data does not match expected")
	})

	t.Run("seeking & reading", func(t *testing.T) {
		buf := NewBufReadWriteSeeker()
		data := []byte("hello, world!")
		_, err := buf.Write(data)
		require.NoError(t, err)

		_, err = buf.Seek(0, io.SeekStart)
		require.NoError(t, err)

		written, err := io.ReadAll(buf)
		require.NoError(t, err)
		require.Equal(t, data, written, "data does not match expected")
	})

	t.Run("overwriting", func(t *testing.T) {
		buf := NewBufReadWriteSeeker()
		data := []byte("hello, world!")
		_, err := buf.Write(data)
		require.NoError(t, err)

		_, err = buf.Seek(-6, io.SeekCurrent)
		require.NoError(t, err)

		_, err = buf.Write([]byte("wrold!"))
		require.NoError(t, err)

		_, err = buf.Seek(0, io.SeekStart)
		require.NoError(t, err)

		expected := []byte("hello, wrold!")
		require.Equal(t, expected, buf.Bytes(), "data does not match expected")
	})
}
