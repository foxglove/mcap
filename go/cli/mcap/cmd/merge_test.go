package cmd

import (
	"bytes"
	"io"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
)

func prepInput(t *testing.T, w io.Writer, schemaID uint16, channelID uint16, topic string) {
	writer, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Chunked: true,
	})
	assert.Nil(t, err)

	assert.Nil(t, writer.WriteHeader(&mcap.Header{}))
	assert.Nil(t, writer.WriteSchema(&mcap.Schema{
		ID: schemaID,
	}))
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:       channelID,
		SchemaID: schemaID,
		Topic:    topic,
	}))
	for i := 0; i < 100; i++ {
		assert.Nil(t, writer.WriteMessage(&mcap.Message{
			ChannelID: channelID,
			LogTime:   uint64(i),
		}))
	}
	assert.Nil(t, writer.Close())
}

func TestMCAPMerging(t *testing.T) {
	for _, chunked := range []bool{true, false} {
		buf1 := &bytes.Buffer{}
		buf2 := &bytes.Buffer{}
		buf3 := &bytes.Buffer{}
		prepInput(t, buf1, 1, 1, "/foo")
		prepInput(t, buf2, 1, 1, "/bar")
		prepInput(t, buf3, 1, 1, "/baz")
		merger := newMCAPMerger(mergeOpts{
			chunked: chunked,
		})
		output := &bytes.Buffer{}
		assert.Nil(t, merger.mergeInputs(output, []io.Reader{buf1, buf2, buf3}))

		// output should now be a well-formed mcap
		reader, err := mcap.NewReader(output)
		assert.Nil(t, err)
		it, err := reader.Content(mcap.WithAllMessages())
		assert.Nil(t, err)

		messages := make(map[string]int)
		err = mcap.Range(it, func(contentRecord mcap.ContentRecord) error {
			message := contentRecord.AsMessage()
			messages[message.Channel.Topic]++
			return nil
		})
		assert.Nil(t, err)
		assert.Equal(t, 100, messages["/foo"])
		assert.Equal(t, 100, messages["/bar"])
		assert.Equal(t, 100, messages["/baz"])
	}
}

func TestMultiChannelInput(t *testing.T) {
	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}
	prepInput(t, buf1, 1, 1, "/foo")
	prepInput(t, buf2, 1, 1, "/bar")
	merger := newMCAPMerger(mergeOpts{})
	multiChannelInput := &bytes.Buffer{}
	assert.Nil(t, merger.mergeInputs(multiChannelInput, []io.Reader{buf1, buf2}))
	buf3 := &bytes.Buffer{}
	prepInput(t, buf3, 2, 2, "/baz")
	output := &bytes.Buffer{}
	assert.Nil(t, merger.mergeInputs(output, []io.Reader{multiChannelInput, buf3}))
	reader, err := mcap.NewReader(output)
	assert.Nil(t, err)
	it, err := reader.Content(mcap.WithAllMessages())
	assert.Nil(t, err)
	messages := make(map[string]int)
	err = mcap.Range(it, func(contentRecord mcap.ContentRecord) error {
		channel := contentRecord.AsMessage().Channel
		messages[channel.Topic]++
		return nil
	})
	assert.Nil(t, err)
	assert.Equal(t, 100, messages["/foo"])
	assert.Equal(t, 100, messages["/bar"])
	assert.Equal(t, 100, messages["/baz"])
}
