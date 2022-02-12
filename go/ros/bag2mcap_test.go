package ros

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
)

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
