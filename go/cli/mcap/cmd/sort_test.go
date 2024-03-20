package cmd

import (
	"bytes"
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

	// verify it is now sorted
	r, err := mcap.NewReader(bytes.NewReader(w.Bytes()))
	require.NoError(t, err)

	it, err := r.Messages(mcap.UsingIndex(false))
	require.NoError(t, err)

	_, _, msg, err := it.Next(nil)
	require.NoError(t, err)
	assert.Equal(t, 25, int(msg.LogTime))
}
