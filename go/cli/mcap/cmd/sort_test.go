package cmd

import (
	"bytes"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
)

func TestSortFile(t *testing.T) {
	buf := &bytes.Buffer{}
	writer, err := mcap.NewWriter(buf, &mcap.WriterOptions{
		Chunked: true,
	})
	assert.Nil(t, err)
	assert.Nil(t, writer.WriteHeader(&mcap.Header{}))
	assert.Nil(t, writer.WriteSchema(&mcap.Schema{
		ID:       1,
		Name:     "foo",
		Encoding: "ros1",
		Data:     []byte{},
	}))
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:              0,
		SchemaID:        1,
		Topic:           "/foo",
		MessageEncoding: "ros1msg",
	}))
	assert.Nil(t, writer.WriteMessage(&mcap.Message{
		ChannelID:   0,
		Sequence:    0,
		LogTime:     100,
		PublishTime: 0,
		Data:        []byte{},
	}))
	assert.Nil(t, writer.WriteMessage(&mcap.Message{
		ChannelID:   0,
		Sequence:    0,
		LogTime:     50,
		PublishTime: 0,
		Data:        []byte{},
	}))
	assert.Nil(t, writer.Close())

	// sort the file
	reader := bytes.NewReader(buf.Bytes())
	w := &bytes.Buffer{}
	assert.Nil(t, sortFile(w, reader))

	// verify it is now sorted
	r, err := mcap.NewReader(bytes.NewReader(w.Bytes()))
	assert.Nil(t, err)

	it, err := r.Messages(mcap.UsingIndex(false))
	assert.Nil(t, err)

	_, _, msg, err := it.Next(nil)
	assert.Nil(t, err)
	assert.Equal(t, 50, int(msg.LogTime))
}
