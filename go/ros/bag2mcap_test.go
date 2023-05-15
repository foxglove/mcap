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

func TestConvertsBz2(t *testing.T) {
	inputfile := "testdata/markers.bz2.bag"
	f, err := os.Open(inputfile)
	assert.Nil(t, err)
	opts := &mcap.WriterOptions{
		IncludeCRC:  true,
		Chunked:     true,
		ChunkSize:   4 * 1024 * 1024,
		Compression: "",
	}
	output := &bytes.Buffer{}
	assert.Nil(t, Bag2MCAP(output, f, opts))

	reader, err := mcap.NewReader(bytes.NewReader(output.Bytes()))
	assert.Nil(t, err)
	info, err := reader.Info()
	assert.Nil(t, err)
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
			assert.ErrorIs(t, err, c.expectedErr)
			assert.Equal(t, c.expectedChannelID, channelID)
		})
	}
}
