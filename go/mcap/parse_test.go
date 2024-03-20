package mcap

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseHeader(t *testing.T) {
	cases := []struct {
		assertion string
		input     []byte
		output    *Header
		err       error
	}{
		{
			"missing profile",
			[]byte{},
			nil,
			io.ErrShortBuffer,
		},
		{
			"missing library",
			prefixedString("ros1"),
			nil,
			io.ErrShortBuffer,
		},
		{
			"valid header",
			flatten(prefixedString("ros1"), prefixedString("library")),
			&Header{
				Profile: "ros1",
				Library: "library",
			},
			nil,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			output, err := ParseHeader(c.input)
			require.ErrorIs(t, err, c.err)
			require.Equal(t, output, c.output)
		})
	}
}

func TestParseMetadata(t *testing.T) {
	cases := []struct {
		assertion string
		input     []byte
		output    *Metadata
		err       error
	}{
		{
			"empty input",
			[]byte{},
			nil,
			io.ErrShortBuffer,
		},
		{
			"missing metadata",
			prefixedString("metadata"),
			nil,
			io.ErrShortBuffer,
		},
		{
			"empty metadata",
			flatten(prefixedString("metadata"), makePrefixedMap(map[string]string{})),
			&Metadata{
				Name:     "metadata",
				Metadata: make(map[string]string),
			},
			nil,
		},
		{
			"one value",
			flatten(prefixedString("metadata"), makePrefixedMap(map[string]string{
				"foo": "bar",
			})),
			&Metadata{
				Name: "metadata",
				Metadata: map[string]string{
					"foo": "bar",
				},
			},
			nil,
		},
		{
			"two values",
			flatten(prefixedString("metadata"), makePrefixedMap(map[string]string{
				"foo":  "bar",
				"spam": "eggs",
			})),
			&Metadata{
				Name: "metadata",
				Metadata: map[string]string{
					"foo":  "bar",
					"spam": "eggs",
				},
			},
			nil,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			output, err := ParseMetadata(c.input)
			require.ErrorIs(t, err, c.err)
			require.Equal(t, output, c.output)
		})
	}
}

func TestParseMetadataIndex(t *testing.T) {
	cases := []struct {
		assertion string
		input     []byte
		output    *MetadataIndex
		err       error
	}{
		{
			"empty input",
			[]byte{},
			nil,
			io.ErrShortBuffer,
		},
		{
			"offset only",
			encodedUint64(100),
			nil,
			io.ErrShortBuffer,
		},
		{
			"missing name",
			flatten(encodedUint64(100), encodedUint64(1000)),
			nil,
			io.ErrShortBuffer,
		},
		{
			"well-formed index",
			flatten(encodedUint64(100), encodedUint64(1000), prefixedString("metadata")),
			&MetadataIndex{
				Name:   "metadata",
				Offset: 100,
				Length: 1000,
			},
			nil,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			output, err := ParseMetadataIndex(c.input)
			require.ErrorIs(t, err, c.err)
			require.Equal(t, output, c.output)
		})
	}
}

func TestParseFooter(t *testing.T) {
	cases := []struct {
		assertion string
		input     []byte
		output    *Footer
		err       error
	}{
		{
			"short summary start",
			[]byte{},
			nil,
			io.ErrShortBuffer,
		},
		{
			"short summary offset start",
			encodedUint64(100),
			nil,
			io.ErrShortBuffer,
		},
		{
			"short crc",
			flatten(encodedUint64(100), encodedUint64(10000)),
			nil,
			io.ErrShortBuffer,
		},
		{
			"valid footer",
			flatten(encodedUint64(1), encodedUint64(2), encodedUint32(20)),
			&Footer{
				SummaryStart:       1,
				SummaryOffsetStart: 2,
				SummaryCRC:         20,
			},
			nil,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			output, err := ParseFooter(c.input)
			require.ErrorIs(t, err, c.err)
			require.Equal(t, output, c.output)
		})
	}
}

func TestParseSchema(t *testing.T) {
	cases := []struct {
		assertion string
		input     []byte
		output    *Schema
		err       error
	}{
		{
			"short schema ID",
			[]byte{},
			nil,
			io.ErrShortBuffer,
		},
		{
			"short schema name",
			encodedUint16(10),
			nil,
			io.ErrShortBuffer,
		},
		{
			"short encoding",
			flatten(encodedUint16(1), prefixedString("schema")),
			nil,
			io.ErrShortBuffer,
		},
		{
			"short data",
			flatten(encodedUint16(10), prefixedString("schema"), prefixedString("encoding")),
			nil,
			io.ErrShortBuffer,
		},
		{
			"valid schema",
			flatten(
				encodedUint16(10),
				prefixedString("schema"),
				prefixedString("encoding"),
				prefixedBytes([]byte{0x99}),
			),
			&Schema{
				ID:       10,
				Name:     "schema",
				Encoding: "encoding",
				Data:     []byte{0x99},
			},
			nil,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			output, err := ParseSchema(c.input)
			require.ErrorIs(t, err, c.err)
			require.Equal(t, output, c.output)
		})
	}
}
