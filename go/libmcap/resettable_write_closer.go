package libmcap

import (
	"bytes"
	"io"
)

// ResettableWriteCloser is a WriteCloser that supports a Reset method.
type ResettableWriteCloser interface {
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

func (b bufCloser) Reset(w io.Writer) {
	b.b.Reset()
}

// NewResettableBufCloser returns a ResettableWriteCloser backed by a
// bytes.Buffer.
func NewResettableBufCloser(buf *bytes.Buffer) ResettableWriteCloser {
	return &bufCloser{
		b: buf,
	}
}
