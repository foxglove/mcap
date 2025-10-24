package cmd

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecover(t *testing.T) {
	t.Run("recover data from truncated file", func(t *testing.T) {
		writeBuf := bytes.Buffer{}
		readBuf := bytes.Buffer{}
		writeFilterTestInput(t, &readBuf)
		readBuf.Truncate(readBuf.Len() / 2)

		require.NoError(t, recoverRun(&readBuf, &writeBuf, &recoverOptions{}))

		messageCounter := map[uint16]int{
			1: 0,
			2: 0,
			3: 0,
		}
		attachmentCounter := 0
		metadataCounter := 0
		lexer, err := mcap.NewLexer(&writeBuf, &mcap.LexerOptions{
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
		writeBuf := bytes.Buffer{}
		readBuf := bytes.Buffer{}
		writeFilterTestInput(t, &readBuf)
		readBuf.Bytes()[0x12b] = 1 // overwrite crc

		require.NoError(t, recoverRun(&readBuf, &writeBuf, &recoverOptions{}))
		messageCounter := map[uint16]int{
			1: 0,
			2: 0,
			3: 0,
		}
		attachmentCounter := 0
		metadataCounter := 0
		lexer, err := mcap.NewLexer(&writeBuf, &mcap.LexerOptions{
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

func TestValidateRecoveredFile(t *testing.T) {
	t.Run("valid recovered file passes validation", func(t *testing.T) {
		writeBuf := bytes.Buffer{}
		readBuf := bytes.Buffer{}
		writeFilterTestInput(t, &readBuf)

		// Recover from a valid file
		require.NoError(t, recoverRun(&readBuf, &writeBuf, &recoverOptions{}))

		// Validate the recovered file
		reader := bytes.NewReader(writeBuf.Bytes())
		err := validateRecoveredFile(reader)
		assert.NoError(t, err, "valid recovered file should pass validation")
	})

	t.Run("recovered file from truncated input passes validation", func(t *testing.T) {
		writeBuf := bytes.Buffer{}
		readBuf := bytes.Buffer{}
		writeFilterTestInput(t, &readBuf)
		readBuf.Truncate(readBuf.Len() / 2)

		// Recover from truncated file
		require.NoError(t, recoverRun(&readBuf, &writeBuf, &recoverOptions{}))

		// Validate the recovered file
		reader := bytes.NewReader(writeBuf.Bytes())
		err := validateRecoveredFile(reader)
		assert.NoError(t, err, "recovered file from truncated input should still be valid")
	})

	t.Run("recovered file from invalid CRC passes validation", func(t *testing.T) {
		writeBuf := bytes.Buffer{}
		readBuf := bytes.Buffer{}
		writeFilterTestInput(t, &readBuf)
		readBuf.Bytes()[0x12b] = 1 // overwrite crc

		// Recover from file with invalid CRC
		require.NoError(t, recoverRun(&readBuf, &writeBuf, &recoverOptions{}))

		// Validate the recovered file
		reader := bytes.NewReader(writeBuf.Bytes())
		err := validateRecoveredFile(reader)
		assert.NoError(t, err, "recovered file from invalid CRC should be valid")
	})

	t.Run("invalid MCAP file fails validation", func(t *testing.T) {
		invalidData := []byte("this is not a valid MCAP file")
		reader := bytes.NewReader(invalidData)

		err := validateRecoveredFile(reader)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid MCAP file")
	})

	t.Run("truncated recovered file fails validation", func(t *testing.T) {
		writeBuf := bytes.Buffer{}
		readBuf := bytes.Buffer{}
		writeFilterTestInput(t, &readBuf)

		// Create a valid recovered file
		require.NoError(t, recoverRun(&readBuf, &writeBuf, &recoverOptions{}))

		// Truncate it to remove the footer
		truncatedData := writeBuf.Bytes()[:len(writeBuf.Bytes())-100]
		reader := bytes.NewReader(truncatedData)

		err := validateRecoveredFile(reader)
		assert.Error(t, err)
		// Could be either "cannot be read" or "incomplete: missing footer"
		assert.True(t,
			strings.Contains(err.Error(), "cannot be read") ||
				strings.Contains(err.Error(), "incomplete: missing footer"),
			"error should indicate file is unreadable or incomplete, got: %s", err.Error())
	})

	t.Run("empty file fails validation", func(t *testing.T) {
		reader := bytes.NewReader([]byte{})

		err := validateRecoveredFile(reader)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid MCAP file")
	})

	t.Run("file with valid header but no footer fails validation", func(t *testing.T) {
		// Create a minimal MCAP file with just a header
		buf := bytes.Buffer{}
		writer, err := mcap.NewWriter(&buf, &mcap.WriterOptions{
			Chunked: false,
		})
		require.NoError(t, err)

		// Write header but don't close (which would write footer)
		require.NoError(t, writer.WriteHeader(&mcap.Header{
			Profile: "test",
			Library: "test",
		}))

		reader := bytes.NewReader(buf.Bytes())
		err = validateRecoveredFile(reader)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "incomplete: missing footer")
	})
}
