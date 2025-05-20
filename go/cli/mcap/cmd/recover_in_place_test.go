package cmd

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockFile struct {
	buf []byte // underlying data
	pos int64  // current read/write position
}

// Read reads up to len(p) bytes into p from the buffer at current position.
func (b *mockFile) Read(p []byte) (int, error) {
	if b.pos >= int64(len(b.buf)) {
		return 0, io.EOF
	}
	n := copy(p, b.buf[b.pos:])
	b.pos += int64(n)
	return n, nil
}

// Write writes len(p) bytes from p into the buffer at current position,
// extending the buffer if necessary.
func (b *mockFile) Write(p []byte) (int, error) {
	newPos := b.pos + int64(len(p))
	// extend underlying slice if write goes beyond current length
	if newPos > int64(len(b.buf)) {
		newBuf := make([]byte, newPos)
		copy(newBuf, b.buf)
		b.buf = newBuf
	}
	copy(b.buf[b.pos:newPos], p)
	b.pos = newPos
	return len(p), nil
}

// Seek sets the position for the next Read or Write according to whence.
func (b *mockFile) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = b.pos + offset
	case io.SeekEnd:
		newPos = int64(len(b.buf)) + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}
	if newPos < 0 {
		return 0, fmt.Errorf("negative position not allowed")
	}
	b.pos = newPos
	return b.pos, nil
}

func (b *mockFile) Truncate(size int64) error {
	if size < 0 || size > int64(len(b.buf)) {
		return fmt.Errorf("invalid size: %d", size)
	}
	b.buf = b.buf[:size]
	if b.pos > size {
		b.pos = size
	}
	return nil
}

func TestRecoverInPlace(t *testing.T) {
	t.Run("recover in place data from truncated file", func(t *testing.T) {
		readBuf := bytes.Buffer{}
		writeFilterTestInput(t, &readBuf)
		readBuf.Truncate(readBuf.Len() / 2)

		file := &mockFile{buf: readBuf.Bytes()}

		require.NoError(t, recoverInPlace(file, false))

		messageCounter := map[uint16]int{
			1: 0,
			2: 0,
			3: 0,
		}
		attachmentCounter := 0
		metadataCounter := 0

		_, err := file.Seek(0, io.SeekStart)
		require.NoError(t, err)

		lexer, err := mcap.NewLexer(file, &mcap.LexerOptions{
			AttachmentCallback: func(*mcap.AttachmentReader) error {
				attachmentCounter++
				return nil
			},
		})
		require.NoError(t, err)
		defer lexer.Close()
		for {
			token, record, err := lexer.Next(nil)
			if err != nil {
				require.ErrorIs(t, err, io.EOF)
				break
			}
			switch token {
			case mcap.TokenMessage:
				message, err := mcap.ParseMessage(record)
				require.NoError(t, err)
				messageCounter[message.ChannelID]++
			case mcap.TokenMetadata:
				metadataCounter++
			}
		}
		assert.Equal(t, 0, attachmentCounter)
		assert.Equal(t, 0, metadataCounter)
		assert.InDeltaMapValues(t, map[uint16]int{
			1: 88,
			2: 88,
			3: 88,
		}, messageCounter, 0.0)
	})

	t.Run("recover data from chunk with invalid crc", func(t *testing.T) {
		readBuf := bytes.Buffer{}
		writeFilterTestInput(t, &readBuf)
		readBuf.Bytes()[0x12b] = 1 // overwrite crc

		file := &mockFile{buf: readBuf.Bytes()}

		require.NoError(t, recoverInPlace(file, true))
		messageCounter := map[uint16]int{
			1: 0,
			2: 0,
			3: 0,
		}
		attachmentCounter := 0
		metadataCounter := 0

		_, err := file.Seek(0, io.SeekStart)
		require.NoError(t, err)

		lexer, err := mcap.NewLexer(file, &mcap.LexerOptions{
			AttachmentCallback: func(_ *mcap.AttachmentReader) error {
				attachmentCounter++
				return nil
			},
		})
		require.NoError(t, err)
		for {
			token, record, err := lexer.Next(nil)
			if err != nil {
				require.ErrorIs(t, err, io.EOF)
				break
			}
			switch token {
			case mcap.TokenMessage:
				message, err := mcap.ParseMessage(record)
				require.NoError(t, err)
				messageCounter[message.ChannelID]++
			case mcap.TokenMetadata:
				metadataCounter++
			}
		}
		assert.Equal(t, 1, attachmentCounter)
		assert.Equal(t, 1, metadataCounter)
		assert.InDeltaMapValues(t, map[uint16]int{
			1: 100,
			2: 100,
			3: 100,
		}, messageCounter, 0.0)
	})
}
