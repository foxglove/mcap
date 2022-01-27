package libmcap

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func BenchmarkMessageIteration(b *testing.B) {
	b.Run("indexed - memory", func(b *testing.B) {
		bagfile, err := os.Open("testdata/demo.bag")
		assert.Nil(b, err)
		defer bagfile.Close()

		mcapfile := &bytes.Buffer{}
		err = Bag2MCAP(bagfile, mcapfile)
		assert.Nil(b, err)
		r, err := NewReader(bytes.NewReader(mcapfile.Bytes()))
		assert.Nil(b, err)
		it, err := r.Messages(0, time.Now().UnixNano(), []string{}, true)
		assert.Nil(b, err)
		c := 0
		for {
			_, _, err := it.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			assert.Nil(b, err)
			c++
		}
		assert.Equal(b, 1606, c)
	})
}
