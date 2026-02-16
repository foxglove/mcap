package cmd

import (
	"bytes"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeDuTestFile writes an MCAP file with known message data sizes for testing.
// Returns the raw bytes of the file.
func writeDuTestFile(t *testing.T, opts *mcap.WriterOptions, messages []struct {
	channelID uint16
	logTime   uint64
	data      []byte
}, channels []struct {
	id    uint16
	topic string
}) []byte {
	t.Helper()
	buf := &bytes.Buffer{}
	writer, err := mcap.NewWriter(buf, opts)
	require.NoError(t, err)

	require.NoError(t, writer.WriteHeader(&mcap.Header{}))
	require.NoError(t, writer.WriteSchema(&mcap.Schema{
		ID:       1,
		Name:     "test_schema",
		Encoding: "raw",
		Data:     []byte{},
	}))

	for _, ch := range channels {
		require.NoError(t, writer.WriteChannel(&mcap.Channel{
			ID:       ch.id,
			SchemaID: 1,
			Topic:    ch.topic,
		}))
	}

	for _, msg := range messages {
		require.NoError(t, writer.WriteMessage(&mcap.Message{
			ChannelID: msg.channelID,
			LogTime:   msg.logTime,
			Data:      msg.data,
		}))
	}

	require.NoError(t, writer.Close())
	return buf.Bytes()
}

func TestDuFromIndexMatchesScan(t *testing.T) {
	channels := []struct {
		id    uint16
		topic string
	}{
		{1, "/camera"},
		{2, "/lidar"},
		{3, "/imu"},
	}

	// Create messages with known data sizes.
	var messages []struct {
		channelID uint16
		logTime   uint64
		data      []byte
	}

	// /camera: 10 messages * 1000 bytes = 10000 bytes
	for i := 0; i < 10; i++ {
		messages = append(messages, struct {
			channelID uint16
			logTime   uint64
			data      []byte
		}{1, uint64(i * 3), make([]byte, 1000)})
	}

	// /lidar: 10 messages * 500 bytes = 5000 bytes
	for i := 0; i < 10; i++ {
		messages = append(messages, struct {
			channelID uint16
			logTime   uint64
			data      []byte
		}{2, uint64(i*3 + 1), make([]byte, 500)})
	}

	// /imu: 20 messages * 100 bytes = 2000 bytes
	for i := 0; i < 20; i++ {
		messages = append(messages, struct {
			channelID uint16
			logTime   uint64
			data      []byte
		}{3, uint64(i*3 + 2), make([]byte, 100)})
	}

	data := writeDuTestFile(t, &mcap.WriterOptions{
		Chunked:   true,
		ChunkSize: 2048,
	}, messages, channels)

	// Run scan (old) path.
	scanUsage := newUsage(bytes.NewReader(data))
	require.NoError(t, scanUsage.RunDu())

	// Run index-based path.
	indexReader := bytes.NewReader(data)
	reader, err := mcap.NewReader(indexReader)
	require.NoError(t, err)
	defer reader.Close()
	info, err := reader.Info()
	require.NoError(t, err)
	require.NotNil(t, info.Footer)
	require.NotZero(t, info.Footer.SummaryStart)
	require.NotEmpty(t, info.ChunkIndexes)

	channelTopics := make(map[uint16]string)
	for id, ch := range info.Channels {
		channelTopics[id] = ch.Topic
	}

	indexTopicSizes, indexTotalMsgSize, err := computeTopicSizesFromIndex(
		indexReader, info.ChunkIndexes, channelTopics,
	)
	require.NoError(t, err)

	// Verify per-topic sizes match between index-based and scan paths.
	for topic, scanSize := range scanUsage.topicMessageSize {
		indexSize, ok := indexTopicSizes[topic]
		assert.True(t, ok, "topic %s missing from index path", topic)
		assert.Equal(t, scanSize, indexSize,
			"topic %s: scan=%d index=%d", topic, scanSize, indexSize)
	}

	// Verify total message sizes match.
	assert.Equal(t, scanUsage.totalMessageSize, indexTotalMsgSize,
		"total message size: scan=%d index=%d",
		scanUsage.totalMessageSize, indexTotalMsgSize)

	// Verify exact known sizes.
	assert.Equal(t, uint64(10000), indexTopicSizes["/camera"])
	assert.Equal(t, uint64(5000), indexTopicSizes["/lidar"])
	assert.Equal(t, uint64(2000), indexTopicSizes["/imu"])
	assert.Equal(t, uint64(17000), indexTotalMsgSize)
}

func TestDuFromIndexSingleChunk(t *testing.T) {
	channels := []struct {
		id    uint16
		topic string
	}{
		{1, "/data"},
	}

	messages := []struct {
		channelID uint16
		logTime   uint64
		data      []byte
	}{
		{1, 0, make([]byte, 50)},
		{1, 1, make([]byte, 100)},
		{1, 2, make([]byte, 200)},
	}

	data := writeDuTestFile(t, &mcap.WriterOptions{
		Chunked:   true,
		ChunkSize: 1024 * 1024, // large chunk size to get a single chunk
	}, messages, channels)

	// Run scan path.
	scanUsage := newUsage(bytes.NewReader(data))
	require.NoError(t, scanUsage.RunDu())

	// Run index-based path.
	indexReader := bytes.NewReader(data)
	reader, err := mcap.NewReader(indexReader)
	require.NoError(t, err)
	defer reader.Close()
	info, err := reader.Info()
	require.NoError(t, err)

	channelTopics := make(map[uint16]string)
	for id, ch := range info.Channels {
		channelTopics[id] = ch.Topic
	}

	indexTopicSizes, indexTotalMsgSize, err := computeTopicSizesFromIndex(
		indexReader, info.ChunkIndexes, channelTopics,
	)
	require.NoError(t, err)

	assert.Equal(t, scanUsage.topicMessageSize["/data"], indexTopicSizes["/data"])
	assert.Equal(t, uint64(350), indexTotalMsgSize)
}

func TestDuFromIndexFallbackNoSummary(t *testing.T) {
	// An unchunked file has no summary section, so the index-based path
	// should fall back to the scan path.
	channels := []struct {
		id    uint16
		topic string
	}{
		{1, "/test"},
	}

	messages := []struct {
		channelID uint16
		logTime   uint64
		data      []byte
	}{
		{1, 0, make([]byte, 42)},
	}

	data := writeDuTestFile(t, &mcap.WriterOptions{
		Chunked: false,
	}, messages, channels)

	// runDuFromIndex should fall back and not error.
	err := runDuFromIndex(bytes.NewReader(data))
	require.NoError(t, err)
}

func TestDuFromIndexMultipleChannelsPerChunk(t *testing.T) {
	channels := []struct {
		id    uint16
		topic string
	}{
		{1, "/alpha"},
		{2, "/beta"},
	}

	// Interleave messages from two channels within the same chunk.
	messages := []struct {
		channelID uint16
		logTime   uint64
		data      []byte
	}{
		{1, 0, make([]byte, 300)},
		{2, 1, make([]byte, 700)},
		{1, 2, make([]byte, 300)},
		{2, 3, make([]byte, 700)},
	}

	data := writeDuTestFile(t, &mcap.WriterOptions{
		Chunked:   true,
		ChunkSize: 1024 * 1024,
	}, messages, channels)

	scanUsage := newUsage(bytes.NewReader(data))
	require.NoError(t, scanUsage.RunDu())

	indexReader := bytes.NewReader(data)
	reader, err := mcap.NewReader(indexReader)
	require.NoError(t, err)
	defer reader.Close()
	info, err := reader.Info()
	require.NoError(t, err)

	channelTopics := make(map[uint16]string)
	for id, ch := range info.Channels {
		channelTopics[id] = ch.Topic
	}

	indexTopicSizes, _, err := computeTopicSizesFromIndex(
		indexReader, info.ChunkIndexes, channelTopics,
	)
	require.NoError(t, err)

	assert.Equal(t, scanUsage.topicMessageSize["/alpha"], indexTopicSizes["/alpha"])
	assert.Equal(t, scanUsage.topicMessageSize["/beta"], indexTopicSizes["/beta"])
}

func TestDuFromIndexInterleavedNonMessageRecords(t *testing.T) {
	// This test writes Schema and Channel records between messages within a
	// single chunk. The index-based path uses offset differencing which attributes
	// the interleaved non-message record bytes to the preceding message's
	// data size, causing over-estimation. The scan path is always exact.
	buf := &bytes.Buffer{}
	writer, err := mcap.NewWriter(buf, &mcap.WriterOptions{
		Chunked:   true,
		ChunkSize: 1024 * 1024, // large to keep everything in one chunk
	})
	require.NoError(t, err)

	require.NoError(t, writer.WriteHeader(&mcap.Header{}))

	// Schema and channel for /topic_a.
	require.NoError(t, writer.WriteSchema(&mcap.Schema{
		ID: 1, Name: "schema1", Encoding: "raw", Data: []byte{},
	}))
	require.NoError(t, writer.WriteChannel(&mcap.Channel{
		ID: 1, SchemaID: 1, Topic: "/topic_a",
	}))

	// First message on /topic_a.
	require.NoError(t, writer.WriteMessage(&mcap.Message{
		ChannelID: 1, LogTime: 0, Data: make([]byte, 100),
	}))

	// Interleaved: define schema2 and channel2 between messages.
	require.NoError(t, writer.WriteSchema(&mcap.Schema{
		ID: 2, Name: "schema2", Encoding: "raw", Data: []byte{},
	}))
	require.NoError(t, writer.WriteChannel(&mcap.Channel{
		ID: 2, SchemaID: 2, Topic: "/topic_b",
	}))

	// Message on /topic_b.
	require.NoError(t, writer.WriteMessage(&mcap.Message{
		ChannelID: 2, LogTime: 1, Data: make([]byte, 200),
	}))

	require.NoError(t, writer.Close())
	data := buf.Bytes()

	// Scan path: exact results.
	scanUsage := newUsage(bytes.NewReader(data))
	require.NoError(t, scanUsage.RunDu())
	assert.Equal(t, uint64(100), scanUsage.topicMessageSize["/topic_a"])
	assert.Equal(t, uint64(200), scanUsage.topicMessageSize["/topic_b"])

	// Index-based path.
	indexReader := bytes.NewReader(data)
	reader, err := mcap.NewReader(indexReader)
	require.NoError(t, err)
	defer reader.Close()
	info, err := reader.Info()
	require.NoError(t, err)

	channelTopics := make(map[uint16]string)
	for id, ch := range info.Channels {
		channelTopics[id] = ch.Topic
	}

	indexTopicSizes, indexTotal, err := computeTopicSizesFromIndex(
		indexReader, info.ChunkIndexes, channelTopics,
	)
	require.NoError(t, err)

	// /topic_a is over-estimated: the interleaved schema2+channel2 records
	// between message1 and message2 are attributed to message1's data.
	assert.Greater(t, indexTopicSizes["/topic_a"], scanUsage.topicMessageSize["/topic_a"],
		"index path should over-estimate /topic_a due to interleaved records")

	// /topic_b is exact: it's the last message in the chunk, so its size is
	// computed from uncompressedSize - offset, which doesn't include any
	// trailing non-message records.
	assert.Equal(t, scanUsage.topicMessageSize["/topic_b"], indexTopicSizes["/topic_b"])

	// Total is over-estimated.
	assert.Greater(t, indexTotal, scanUsage.totalMessageSize,
		"index path total should be >= scan total due to interleaved records")
}

func TestDuFromIndexUnknownChannelID(t *testing.T) {
	// Verify that the index-based path returns an error when a message index
	// references a channel ID not present in the channelTopics map.
	channels := []struct {
		id    uint16
		topic string
	}{
		{1, "/data"},
	}

	messages := []struct {
		channelID uint16
		logTime   uint64
		data      []byte
	}{
		{1, 0, make([]byte, 50)},
	}

	data := writeDuTestFile(t, &mcap.WriterOptions{
		Chunked:   true,
		ChunkSize: 1024 * 1024,
	}, messages, channels)

	indexReader := bytes.NewReader(data)
	reader, err := mcap.NewReader(indexReader)
	require.NoError(t, err)
	defer reader.Close()
	info, err := reader.Info()
	require.NoError(t, err)

	// Pass an empty channelTopics map so channel 1 is unknown.
	emptyTopics := make(map[uint16]string)
	_, _, err = computeTopicSizesFromIndex(
		indexReader, info.ChunkIndexes, emptyTopics,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown channel")
}
