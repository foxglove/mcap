package cmd

import (
	"bytes"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
)

func TestNoErrorOnMessagelessChunks(t *testing.T) {
	buf := bytes.Buffer{}
	writer, err := mcap.NewWriter(&buf, &mcap.WriterOptions{
		Chunked:   true,
		ChunkSize: 10,
	})
	assert.Nil(t, err)
	assert.Nil(t, writer.WriteHeader(&mcap.Header{
		Profile: "",
		Library: "",
	}))
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:       1,
		SchemaID: 0,
		Topic:    "schemaless_topic",
	}))
	assert.Nil(t, writer.Close())

	rs := bytes.NewReader(buf.Bytes())

	doctor := newMcapDoctor(rs)
	err = doctor.Examine()
	assert.Nil(t, err)
}
