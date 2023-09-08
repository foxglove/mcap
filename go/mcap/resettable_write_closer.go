package mcap

import (
	"bytes"
	"io"
)

// resettableWriteCloser is a WriteCloser that supports a Reset method.
type resettableWriteCloser interface {
	io.WriteCloser
	Reset(io.Writer)
}

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
