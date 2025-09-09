package cmd

import (
	"bytes"
	"os"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
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
	diagnosis := doctor.Examine()
	assert.Empty(t, diagnosis.Errors)
}

func TestRequiresDuplicatedSchemasForIndexedMessages(t *testing.T) {
	rs, err := os.Open("../../../../tests/conformance/data/OneMessage/OneMessage-ch-chx-pad.mcap")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rs.Close())
	}()
	doctor := newMcapDoctor(rs)
	diagnosis := doctor.Examine()
	assert.Len(t, diagnosis.Errors, 2)
	assert.Equal(t,
		"Indexed chunk at offset 28 contains messages referencing channel (1) not duplicated in summary section",
		diagnosis.Errors[0],
	)
	assert.Equal(t,
		"Indexed chunk at offset 28 contains messages referencing schema (1) not duplicated in summary section",
		diagnosis.Errors[1],
	)
}

func TestPassesIndexedMessagesWithRepeatedSchemas(t *testing.T) {
	rs, err := os.Open("../../../../tests/conformance/data/OneMessage/OneMessage-ch-chx-pad-rch-rsh.mcap")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rs.Close())
	}()
	doctor := newMcapDoctor(rs)
	diagnosis := doctor.Examine()
	assert.Empty(t, diagnosis.Errors)
}

func TestNoErrorOnSchemalessMessages(t *testing.T) {
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
	require.NoError(t, writer.WriteMessage(&mcap.Message{
		ChannelID:   1,
		Sequence:    0,
		LogTime:     0,
		PublishTime: 0,
		Data:        []byte{0, 1, 2},
	}))
	require.NoError(t, writer.Close())

	rs := bytes.NewReader(buf.Bytes())

	doctor := newMcapDoctor(rs)
	diagnosis := doctor.Examine()
	assert.Empty(t, diagnosis.Errors)
}
