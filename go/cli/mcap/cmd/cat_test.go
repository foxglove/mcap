package cmd

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
)

func TestCat(t *testing.T) {
	cases := []struct {
		assertion string
		inputfile string
		expected  string
	}{
		{
			"OneMessage",
			"../../../../tests/conformance/data/OneMessage/OneMessage-ch-chx-mx-pad-rch-rsh-st-sum.mcap",
			"2 example [Example] [1 2 3]\n",
		},
	}
	for _, c := range cases {
		input, err := os.ReadFile(c.inputfile)
		assert.Nil(t, err)
		w := new(bytes.Buffer)
		r := bytes.NewReader(input)
		t.Run(c.assertion, func(t *testing.T) {
			reader, err := mcap.NewReader(r)
			assert.Nil(t, err)
			defer reader.Close()
			it, err := reader.Messages()
			assert.Nil(t, err)
			err = printMessages(w, it, false)
			assert.Nil(t, err)
			r.Reset(input)
			assert.Equal(t, c.expected, w.String())
		})
	}
}

func BenchmarkCat(b *testing.B) {
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
		input, err := os.ReadFile(c.inputfile)
		assert.Nil(b, err)
		w := io.Discard
		r := bytes.NewReader(input)
		b.Run(c.assertion, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				func() {
					reader, err := mcap.NewReader(r)
					assert.Nil(b, err)
					defer reader.Close()
					it, err := reader.Messages()
					assert.Nil(b, err)
					err = printMessages(w, it, c.formatJSON)
					assert.Nil(b, err)
					r.Reset(input)
				}()
			}
		})
	}
}
