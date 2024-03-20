package cmd

import (
	"bytes"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/require"
)

func TestNoErrorOnMessagelessChunks(t *testing.T) {
	buf := bytes.Buffer{}
	writer, err := mcap.NewWriter(&buf, &mcap.WriterOptions{
		Chunked:   true,
		ChunkSize: 10,
	})
	require.NoError(t, err)
	require.NoError(t, writer.WriteHeader(&mcap.Header{
		Profile: "",
		Library: "",
	}))
	require.NoError(t, writer.WriteChannel(&mcap.Channel{
		ID:       1,
		SchemaID: 0,
		Topic:    "schemaless_topic",
	}))
	require.NoError(t, writer.Close())

	rs := bytes.NewReader(buf.Bytes())

	doctor := newMcapDoctor(rs)
	err = doctor.Examine()
	require.NoError(t, err)
}
