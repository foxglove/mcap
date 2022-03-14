package cmd

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"math"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
)

func BenchmarkCat(b *testing.B) {
	ctx := context.Background()
	cases := []struct {
		assertion  string
		inputfile  string
		formatJSON bool
	}{
		{
			"demo.bag",
			"../../../../testdata/mcap/demo.mcap",
			true,
		},
	}
	for _, c := range cases {
		input, err := ioutil.ReadFile(c.inputfile)
		assert.Nil(b, err)
		w := io.Discard
		r := bytes.NewReader(input)
		b.Run(c.assertion, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				reader, err := mcap.NewReader(r)
				assert.Nil(b, err)
				it, err := reader.Messages(0, math.MaxInt64, []string{}, true)
				assert.Nil(b, err)
				err = printMessages(ctx, w, it, c.formatJSON)
				assert.Nil(b, err)
				r.Reset(input)
			}
		})
	}
}
