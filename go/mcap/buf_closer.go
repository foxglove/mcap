package mcap

import (
	"bytes"
	"io"
)

type bufCloser struct {
	b *bytes.Buffer
}

func (b bufCloser) Close() error {
	return nil
}

func (b bufCloser) Write(p []byte) (int, error) {
	return b.b.Write(p)
}

func (b bufCloser) Reset(_ io.Writer) {
	b.b.Reset()
}
