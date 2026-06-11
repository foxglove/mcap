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

func makeMergeInput(t *testing.T, msgs []struct {
	logTime uint64
	data    string
}) io.ReadSeeker {
	t.Helper()
	buf := &bytes.Buffer{}
	w, err := mcap.NewWriter(buf, &mcap.WriterOptions{
		Chunked:     true,
		ChunkSize:   1024,
		Compression: mcap.CompressionZSTD,
		IncludeCRC:  true,
	})
	require.NoError(t, err)
	require.NoError(t, w.WriteHeader(&mcap.Header{Profile: "test"}))
	require.NoError(t, w.WriteSchema(&mcap.Schema{ID: 1, Name: "Msg", Encoding: "ros1msg", Data: []byte("string data")}))
	require.NoError(t, w.WriteChannel(&mcap.Channel{ID: 1, SchemaID: 1, Topic: "/chatter", MessageEncoding: "ros1"}))
	for _, m := range msgs {
		require.NoError(t, w.WriteMessage(&mcap.Message{ChannelID: 1, LogTime: m.logTime, Data: []byte(m.data)}))
	}
	require.NoError(t, w.Close())
	return bytes.NewReader(buf.Bytes())
}

func collectMessages(t *testing.T, r io.Reader) []uint64 {
	t.Helper()
	reader, err := mcap.NewReader(r.(io.ReadSeeker))
	require.NoError(t, err)
	defer reader.Close()
	it, err := reader.Messages(mcap.UsingIndex(false))
	require.NoError(t, err)
	var times []uint64
	for {
		_, _, msg, err := it.NextInto(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		times = append(times, msg.LogTime)
	}
	return times
}

func TestMergeParallelWorkers(t *testing.T) {
	msgs1 := []struct {
		logTime uint64
		data    string
	}{{1, "a"}, {3, "c"}, {5, "e"}}
	msgs2 := []struct {
		logTime uint64
		data    string
	}{{2, "b"}, {4, "d"}, {6, "f"}}

	expected := []uint64{1, 2, 3, 4, 5, 6}

	for _, workers := range []int{1, 2, 4, 0} {
		t.Run("", func(t *testing.T) {
			inputs := []namedReader{
				{name: "input1", reader: makeMergeInput(t, msgs1)},
				{name: "input2", reader: makeMergeInput(t, msgs2)},
			}
			opts := mergeOpts{
				compression:      "zstd",
				chunkSize:        1024 * 1024,
				includeCRC:       true,
				chunked:          true,
				coalesceChannels: "auto",
				workers:          workers,
			}
			merger := newMCAPMerger(opts)
			out := &bytes.Buffer{}
			require.NoError(t, merger.mergeInputs(out, inputs))

			got := collectMessages(t, bytes.NewReader(out.Bytes()))
			assert.Equal(t, expected, got, "workers=%d: timestamps out of order", workers)
		})
	}
}
