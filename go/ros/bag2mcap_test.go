package ros

import (
	"bytes"
	"errors"
	"io"
	"math"
	"os"
	"testing"
	"time"

	"github.com/foxglove/go-rosbag"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBag2MCAPPreservesChannelMetadata(t *testing.T) {
	inputFile := "./testdata/markers.bag"
	expectedKeys := []string{"md5sum", "topic"}
	f, err := os.Open(inputFile)
	require.NoError(t, err)
	writer := &bytes.Buffer{}
	err = Bag2MCAP(writer, f, &mcap.WriterOptions{
		IncludeCRC:  true,
		Chunked:     true,
		ChunkSize:   4 * 1024 * 1024,
		Compression: mcap.CompressionNone,
	})
	require.NoError(t, err)
	lexer, err := mcap.NewLexer(writer)
	require.NoError(t, err)
	channelCount := 0
	for {
		tokenType, token, err := lexer.Next(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		switch tokenType {
		case mcap.TokenChannel:
			ch, err := mcap.ParseChannel(token)
			require.NoError(t, err)
			assert.Equal(t, len(expectedKeys), len(ch.Metadata))
			for _, k := range expectedKeys {
				assert.Contains(t, ch.Metadata, k)
			}
			channelCount++
		default:
		}
	}
	assert.Equal(t, 3, channelCount)
}

func TestDeduplicatesSchemas(t *testing.T) {
	buf := &bytes.Buffer{}
	bw, err := rosbag.NewWriter(buf)
	require.NoError(t, err)
	require.NoError(t, bw.WriteBagHeader(rosbag.BagHeader{}))
	require.NoError(t, bw.WriteConnection(&rosbag.Connection{
		Conn:  0,
		Topic: "yo",
		Data: rosbag.ConnectionHeader{
			Topic:  "yo",
			Type:   "a",
			MD5Sum: "123",
		},
	}))
	require.NoError(t, bw.WriteConnection(&rosbag.Connection{
		Conn:  1,
		Topic: "yoo",
		Data: rosbag.ConnectionHeader{
			Topic:  "yoo",
			Type:   "a",
			MD5Sum: "123",
		},
	}))
	require.NoError(t, bw.WriteMessage(&rosbag.Message{
		Conn: 0,
		Time: 0,
		Data: []byte{},
	}))
	require.NoError(t, bw.WriteMessage(&rosbag.Message{
		Conn: 1,
		Time: 0,
		Data: []byte{},
	}))
	require.NoError(t, bw.Close())

	output := &bytes.Buffer{}
	require.NoError(t, Bag2MCAP(output, buf, &mcap.WriterOptions{
		IncludeCRC: true,
		Chunked:    true,
		ChunkSize:  1024,
	}))

	rs := bytes.NewReader(output.Bytes())
	reader, err := mcap.NewReader(rs)
	require.NoError(t, err)

	info, err := reader.Info()
	require.NoError(t, err)
	assert.Equal(t, 2, int(info.Statistics.ChannelCount))
	assert.Equal(t, 1, int(info.Statistics.SchemaCount))
}

func BenchmarkBag2MCAP(b *testing.B) {
	opts := &mcap.WriterOptions{
		IncludeCRC:  true,
		Chunked:     true,
		ChunkSize:   4 * 1024 * 1024,
		Compression: "",
	}
	cases := []struct {
		assertion string
		inputfile string
	}{
		{
			"demo bag",
			"../../testdata/bags/demo.bag",
		},
	}
	for _, c := range cases {
		stats, err := os.Stat(c.inputfile)
		require.NoError(b, err)
		input, err := os.ReadFile(c.inputfile)
		require.NoError(b, err)
		reader := &bytes.Reader{}
		writer := bytes.NewBuffer(make([]byte, 4*1024*1024*1024))
		b.ResetTimer()
		b.Run(c.assertion, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				t0 := time.Now()
				reader.Reset(input)
				writer.Reset()
				err = Bag2MCAP(writer, reader, opts)
				require.NoError(b, err)
				elapsed := time.Since(t0)
				megabytesRead := stats.Size() / (1024 * 1024)
				b.ReportMetric(float64(megabytesRead)/elapsed.Seconds(), "MB/sec")
			}
		})
	}
}

func TestConvertsBz2(t *testing.T) {
	inputfile := "testdata/markers.bz2.bag"
	f, err := os.Open(inputfile)
	require.NoError(t, err)
	opts := &mcap.WriterOptions{
		IncludeCRC:  true,
		Chunked:     true,
		ChunkSize:   4 * 1024 * 1024,
		Compression: "",
	}
	output := &bytes.Buffer{}
	require.NoError(t, Bag2MCAP(output, f, opts))

	reader, err := mcap.NewReader(bytes.NewReader(output.Bytes()))
	require.NoError(t, err)
	info, err := reader.Info()
	require.NoError(t, err)
	assert.Equal(t, 10, int(info.Statistics.MessageCount))
}

func TestChannelIdForConnection(t *testing.T) {
	cases := []struct {
		label             string
		connID            uint32
		expectedErr       error
		expectedChannelID uint16
	}{
		{
			"less than uint16 MAX",
			10,
			nil,
			10,
		},
		{
			"equal to uint16 max",
			math.MaxUint16,
			nil,
			math.MaxUint16,
		},
		{
			"too much",
			math.MaxUint16 + 1,
			ErrTooManyConnections,
			0,
		},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			channelID, err := channelIDForConnection(c.connID)
			require.ErrorIs(t, err, c.expectedErr)
			assert.Equal(t, c.expectedChannelID, channelID)
		})
	}
}
