package ros

import (
	"bytes"
	"errors"
	"io"
	"math"
	"os"
	"testing"
	"time"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
)

func TestBag2MCAPPreservesChannelMetadata(t *testing.T) {
	inputFile := "./testdata/markers.bag"
	expectedKeys := []string{"md5sum", "topic"}
	f, err := os.Open(inputFile)
	assert.Nil(t, err)
	writer := &bytes.Buffer{}
	err = Bag2MCAP(writer, f, &mcap.WriterOptions{
		IncludeCRC:  true,
		Chunked:     true,
		ChunkSize:   4 * 1024 * 1024,
		Compression: mcap.CompressionNone,
	})
	assert.Nil(t, err)
	lexer, err := mcap.NewLexer(writer)
	assert.Nil(t, err)
	channelCount := 0
	for {
		tokenType, token, err := lexer.Next(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		assert.Nil(t, err)
		switch tokenType {
		case mcap.TokenChannel:
			ch, err := mcap.ParseChannel(token)
			assert.Nil(t, err)
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
		assert.Nil(b, err)
		input, err := os.ReadFile(c.inputfile)
		assert.Nil(b, err)
		reader := &bytes.Reader{}
		writer := bytes.NewBuffer(make([]byte, 4*1024*1024*1024))
		b.ResetTimer()
		b.Run(c.assertion, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				t0 := time.Now()
				reader.Reset(input)
				writer.Reset()
				err = Bag2MCAP(writer, reader, opts)
				assert.Nil(b, err)
				elapsed := time.Since(t0)
				megabytesRead := stats.Size() / (1024 * 1024)
				b.ReportMetric(float64(megabytesRead)/elapsed.Seconds(), "MB/sec")
			}
		})
	}
}

func TestChannelIdForConnection(t *testing.T) {
	cases := []struct {
		label             string
		connId            uint32
		knownConnIds      []uint32
		expectedErr       error
		expectedChannelId uint16
		expectedNewLength int
	}{
		{
			"no known connections",
			10,
			[]uint32{},
			nil,
			0,
			1,
		},
		{
			"adding a new connection",
			10,
			[]uint32{100},
			nil,
			1,
			2,
		},
		{
			"re-using an existing connection",
			10,
			[]uint32{10},
			nil,
			0,
			1,
		},
		{
			"maxed out number of connections, but this connection is known",
			0,
			make([]uint32, math.MaxUint16),
			nil,
			0,
			math.MaxUint16,
		},
		{
			"maxed out number of connections, can't add any more",
			10,
			make([]uint32, math.MaxUint16),
			ErrTooManyConnections,
			0,
			math.MaxUint16,
		},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			channelID, err := channelIDForConnection(&c.knownConnIds, c.connId)
			assert.ErrorIs(t, err, c.expectedErr)
			assert.Equal(t, c.expectedChannelId, channelID)
			assert.Equal(t, c.expectedNewLength, len(c.knownConnIds))
		})
	}
}
