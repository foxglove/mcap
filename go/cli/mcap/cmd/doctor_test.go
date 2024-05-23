package cmd

import (
	"bytes"
	"os"
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

func TestRequiresDuplicatedSchemasForIndexedMessages(t *testing.T) {
	rs, err := os.Open("../../../../tests/conformance/data/OneMessage/OneMessage-ch-chx-pad.mcap")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rs.Close())
	}()
	doctor := newMcapDoctor(rs)
	err = doctor.Examine()
	require.Error(t, err, "encountered 2 errors")
}

func TestPassesIndexedMessagesWithRepeatedSchemas(t *testing.T) {
	rs, err := os.Open("../../../../tests/conformance/data/OneMessage/OneMessage-ch-chx-pad-rch-rsh.mcap")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rs.Close())
	}()
	doctor := newMcapDoctor(rs)
	err = doctor.Examine()
	require.NoError(t, err)
}
