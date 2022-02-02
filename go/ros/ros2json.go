package ros

import "io"

type JSONable interface {
	toJSON(io.Writer, io.Reader) error
}

type ROSJSONTranscoder struct {
	buf []byte
}

type Int8 struct{}

func (x Int8) toJSON(w io.Writer, r io.Reader) error {
	return ""
}

type Uint8 struct{}
type Int16 struct{}
type Uint16 struct{}
type Int32 struct{}
type Uint32 struct{}
type Int64 struct{}
type Uint64 struct{}
type Float32 struct{}
type Float64 struct{}
type String struct{}
type Time struct{}
type Duration struct{}
type Array struct{}
type Record struct{}
