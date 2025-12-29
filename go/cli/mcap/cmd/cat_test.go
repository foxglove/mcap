package cmd

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		{
			"OneSchemalessMessage",
			"../../../../tests/conformance/data/OneSchemalessMessage/OneSchemalessMessage-ch-chx-mx-pad-rch-st.mcap",
			"2 example [no schema] [1 2 3]\n",
		},
	}
	for _, c := range cases {
		input, err := os.ReadFile(c.inputfile)
		require.NoError(t, err)
		w := new(bytes.Buffer)
		r := bytes.NewReader(input)
		t.Run(c.assertion, func(t *testing.T) {
			reader, err := mcap.NewReader(r)
			require.NoError(t, err)
			defer reader.Close()
			it, err := reader.Messages()
			require.NoError(t, err)
			err = printMessages(w, it, false)
			require.NoError(t, err)
			r.Reset(input)
			assert.Equal(t, c.expected, w.String())
		})
	}
}

func TestGetReadOptsUseIndex(t *testing.T) {
	// getReadOpts(false) must set UseIndex=false to allow non-seekable readers (stdin).
	opts := mcap.ReadOptions{UseIndex: true} // default
	for _, opt := range getReadOpts(false) {
		require.NoError(t, opt(&opts))
	}
	assert.False(t, opts.UseIndex)

	// getReadOpts(true) must set UseIndex=true and Order=LogTimeOrder for seekable readers.
	opts = mcap.ReadOptions{}
	for _, opt := range getReadOpts(true) {
		require.NoError(t, opt(&opts))
	}
	assert.True(t, opts.UseIndex)
	assert.Equal(t, mcap.LogTimeOrder, opts.Order)
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
		require.NoError(b, err)
		w := io.Discard
		r := bytes.NewReader(input)
		b.Run(c.assertion, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				func() {
					reader, err := mcap.NewReader(r)
					require.NoError(b, err)
					defer reader.Close()
					it, err := reader.Messages()
					require.NoError(b, err)
					err = printMessages(w, it, c.formatJSON)
					require.NoError(b, err)
					r.Reset(input)
				}()
			}
		})
	}
}
