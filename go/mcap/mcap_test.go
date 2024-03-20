package mcap

import (
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetUint16(t *testing.T) {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, 123)
	t.Run("uint16 successful read", func(t *testing.T) {
		x, offset, err := getUint16(buf, 0)
		require.NoError(t, err)
		require.Equal(t, uint16(123), x)
		require.Equal(t, 2, offset)
	})
	t.Run("uint16 insufficient space", func(t *testing.T) {
		x, offset, err := getUint16(buf, 1)
		require.ErrorIs(t, err, io.ErrShortBuffer)
		require.Equal(t, uint16(0), x)
		require.Equal(t, 0, offset)
	})
	t.Run("uint16 offset outside buffer", func(t *testing.T) {
		x, offset, err := getUint16(buf, 10)
		require.ErrorIs(t, err, io.ErrShortBuffer)
		require.Equal(t, uint16(0), x)
		require.Equal(t, 0, offset)
	})
}

func TestGetUint32(t *testing.T) {
	buf := make([]byte, 4)
	t.Run("uint32 successful read", func(t *testing.T) {
		binary.LittleEndian.PutUint32(buf, 123)
		x, offset, err := getUint32(buf, 0)
		require.NoError(t, err)
		require.Equal(t, uint32(123), x)
		require.Equal(t, 4, offset)
	})
	t.Run("uint32 insufficient space", func(t *testing.T) {
		x, offset, err := getUint32(buf, 1)
		require.ErrorIs(t, err, io.ErrShortBuffer)
		require.Equal(t, uint32(0), x)
		require.Equal(t, 0, offset)
	})
	t.Run("uint32 offset outside buffer", func(t *testing.T) {
		x, offset, err := getUint32(buf, 10)
		require.ErrorIs(t, err, io.ErrShortBuffer)
		require.Equal(t, uint32(0), x)
		require.Equal(t, 0, offset)
	})
}
func TestGetUint64(t *testing.T) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, 123)
	t.Run("uint64 successful read", func(t *testing.T) {
		x, offset, err := getUint64(buf, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(123), x)
		require.Equal(t, 8, offset)
	})
	t.Run("uint64 insufficient space", func(t *testing.T) {
		x, offset, err := getUint64(buf, 1)
		require.ErrorIs(t, err, io.ErrShortBuffer)
		require.Equal(t, uint64(0), x)
		require.Equal(t, 0, offset)
	})
	t.Run("uint64 offset outside buffer", func(t *testing.T) {
		x, offset, err := getUint64(buf, 10)
		require.ErrorIs(t, err, io.ErrShortBuffer)
		require.Equal(t, uint64(0), x)
		require.Equal(t, 0, offset)
	})
}

func TestPutByte(t *testing.T) {
	offset, err := putByte(make([]byte, 1), 123)
	require.NoError(t, err)
	require.Equal(t, 1, offset)
	offset, err = putByte(make([]byte, 0), 123)
	require.ErrorIs(t, err, io.ErrShortBuffer)
	require.Equal(t, 0, offset)
}
