package mcap

import "io"

// ResettableWriteCloser implements io.WriteCloser and adds a Reset method.
type ResettableWriteCloser interface {
	io.WriteCloser
	Reset(io.Writer)
}

// ResettableReadCloser implements io.ReadCloser and adds a Reset method.
type ResettableReader interface {
	io.Reader
	Reset(io.Reader)
}
