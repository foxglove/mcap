package cmd

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortFile(t *testing.T) {
	buf := &bytes.Buffer{}
	writer, err := mcap.NewWriter(buf, &mcap.WriterOptions{
		Chunked: true,
	})
	require.NoError(t, err)
	require.NoError(t, writer.WriteHeader(&mcap.Header{}))
	require.NoError(t, writer.WriteSchema(&mcap.Schema{
		ID:       1,
		Name:     "foo",
		Encoding: "ros1",
		Data:     []byte{},
	}))
	require.NoError(t, writer.WriteChannel(&mcap.Channel{
		ID:              0,
		SchemaID:        1,
		Topic:           "/foo",
		MessageEncoding: "ros1msg",
	}))
	require.NoError(t, writer.WriteChannel(&mcap.Channel{
		ID:              2,
		SchemaID:        0,
		Topic:           "/bar",
		MessageEncoding: "ros1msg",
	}))
	require.NoError(t, writer.WriteMessage(&mcap.Message{
		ChannelID:   0,
		Sequence:    0,
		LogTime:     100,
		PublishTime: 0,
		Data:        []byte{},
	}))
	require.NoError(t, writer.WriteMessage(&mcap.Message{
		ChannelID:   0,
		Sequence:    0,
		LogTime:     50,
		PublishTime: 0,
		Data:        []byte{},
	}))
	require.NoError(t, writer.WriteMessage(&mcap.Message{
		ChannelID:   2,
		Sequence:    0,
		LogTime:     25,
		PublishTime: 0,
		Data:        []byte{},
	}))
	require.NoError(t, writer.Close())

	// sort the file
	reader := bytes.NewReader(buf.Bytes())
	w := &bytes.Buffer{}
	require.NoError(t, sortFile(w, reader))

	lexer, err := mcap.NewLexer(bytes.NewReader(w.Bytes()))
	require.NoError(t, err)
	var schemaCount, channelCount, messageCount int
	var lastMessageTime uint64
top:
	for {
		token, record, err := lexer.Next(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		switch token {
		case mcap.TokenMessage:
			messageCount++
			message, err := mcap.ParseMessage(record)
			require.NoError(t, err)
			require.GreaterOrEqual(t, message.LogTime, lastMessageTime)
			lastMessageTime = message.LogTime
		case mcap.TokenSchema:
			schemaCount++
		case mcap.TokenChannel:
			channelCount++
		case mcap.TokenDataEnd:
			break top
		}
	}
	assert.Equal(t, 1, schemaCount, "incorrect schema count")
	assert.Equal(t, 2, channelCount, "incorrect channel count")
}
