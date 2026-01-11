package cmd

import (
	"bytes"
	"io"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecover(t *testing.T) {
	t.Run("recover data from truncated file", func(t *testing.T) {
		writeBuf := bytes.Buffer{}
		readBuf := bytes.Buffer{}
		writeFilterTestInput(t, &readBuf, true)
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
		writeFilterTestInput(t, &readBuf, true)
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
