package ros

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSONTranscoding(t *testing.T) {
	cases := []struct {
		assertion         string
		parentPackage     string
		messageDefinition string
		input             []byte
		expectedJSON      string
	}{
		{
			"simple string",
			"",
			"string foo",
			[]byte{0x03, 0x00, 0x00, 0x00, 'b', 'a', 'r'},
			`{"foo":"bar"}`,
		},
		{
			"empty string",
			"",
			"string foo",
			[]byte{0x00, 0x00, 0x00, 0x00},
			`{"foo":""}`,
		},
		{
			"string with characters that need to be escaped",
			"",
			"string foo",
			[]byte{0x03, 0x00, 0x00, 0x00, '"', '\n', '\\'},
			`{"foo":"\"\n\\"}`,
		},
		{
			"two primitive fields",
			"",
			`string foo
			int32 bar`,
			[]byte{0x03, 0x00, 0x00, 0x00, 'b', 'a', 'r', 0x01, 0x00, 0x00, 0x00},
			`{"foo":"bar","bar":1}`,
		},
		{
			"primitive variable-length array",
			"",
			`bool[] foo`,
			[]byte{0x01, 0x00, 0x00, 0x00, 0x01},
			`{"foo":[true]}`,
		},
		{
			"primitive fixed-length array",
			"",
			`bool[2] foo`,
			[]byte{0x01, 0x00},
			`{"foo":[true,false]}`,
		},
		{
			"empty primitive array",
			"",
			`bool[] foo`,
			[]byte{0x00, 0x00, 0x00, 0x00},
			`{"foo":[]}`,
		},
		{
			"empty byte array",
			"",
			`uint8[] foo`,
			[]byte{0x00, 0x00, 0x00, 0x00},
			`{"foo":""}`,
		},
		{
			"nonempty byte array",
			"",
			`uint8[] foo`,
			[]byte{0x05, 0x00, 0x00, 0x00, 'h', 'e', 'l', 'l', 'o'},
			`{"foo":"aGVsbG8="}`,
		},
		{
			"dependent type",
			"",
			`Foo foo
			===
			MSG: Foo
			string bar
			`,
			[]byte{0x03, 0x00, 0x00, 0x00, 'b', 'a', 'z'},
			`{"foo":{"bar":"baz"}}`,
		},
		{
			"2x dependent type",
			"",
			`Foo foo
			===
			MSG: Foo
			Baz bar
			===
			MSG: Baz
			string spam
			`,
			[]byte{0x03, 0x00, 0x00, 0x00, 'b', 'a', 'z'},
			`{"foo":{"bar":{"spam":"baz"}}}`,
		},
		{
			"uses a header",
			"",
			`Header header
			===
			MSG: std_msgs/Header
			uint32 seq
			time stamp
			string frame_id
			`,
			[]byte{
				0x01, 0x00, 0x00, 0x00,
				0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
				0x05, 0x00, 0x00, 0x00, 'h', 'e', 'l', 'l', 'o',
			},
			`{"header":{"seq":1,"stamp":16843009.016843009,"frame_id":"hello"}}`,
		},
		{
			"uses a relative type",
			"my_package",
			`MyType foo
			===
			MSG: my_package/MyType
			string bar
			`,
			[]byte{
				0x05, 0x00, 0x00, 0x00, 'h', 'e', 'l', 'l', 'o',
			},
			`{"foo":{"bar":"hello"}}`,
		},
		{
			"relative type inherited from subdefinition",
			"",
			`my_package/MyType foo
			===
			MSG: my_package/MyType
			MyOtherType bar
			==
			MSG: my_package/MyOtherType
			string baz
			`,
			[]byte{
				0x05, 0x00, 0x00, 0x00, 'h', 'e', 'l', 'l', 'o',
			},
			`{"foo":{"bar":{"baz":"hello"}}}`,
		},
		{
			"array of record",
			"",
			`Foo[] foo
			===
			MSG: Foo
			string bar
			string baz
			`,
			[]byte{
				0x02, 0x00, 0x00, 0x00, // two elements
				0x03, 0x00, 0x00, 0x00, 'b', 'a', 'z',
				0x03, 0x00, 0x00, 0x00, 'b', 'a', 'z',
				0x03, 0x00, 0x00, 0x00, 'b', 'a', 'z',
				0x03, 0x00, 0x00, 0x00, 'b', 'a', 'z',
			},
			`{"foo":[{"bar":"baz","baz":"baz"},{"bar":"baz","baz":"baz"}]}`,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			definition := []byte(c.messageDefinition)
			buf := &bytes.Buffer{}
			transcoder, err := NewJSONTranscoder(c.parentPackage, definition)
			assert.Nil(t, err)
			err = transcoder.Transcode(buf, bytes.NewReader(c.input))
			assert.Nil(t, err)
			assert.Equal(t, c.expectedJSON, buf.String())
		})
	}
}

func TestSingleRecordConversion(t *testing.T) {
	transcoder, err := NewJSONTranscoder("", nil)
	assert.Nil(t, err)
	cases := []struct {
		assertion     string
		parentPackage string
		fields        []recordField
		input         []byte
		output        string
	}{
		{
			"string",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.string,
				},
			},
			[]byte{0x03, 0x00, 0x00, 0x00, 'b', 'a', 'r'},
			`{"foo":"bar"}`,
		},
		{
			"bool",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.bool,
				},
			},
			[]byte{0x01},
			`{"foo":true}`,
		},
		{
			"int8",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.int8,
				},
			},
			[]byte{0x01},
			`{"foo":1}`,
		},
		{
			"int16",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.int16,
				},
			},
			[]byte{0x07, 0x07},
			`{"foo":1799}`,
		},
		{
			"int32",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.int32,
				},
			},
			[]byte{0x07, 0x07, 0x07, 0x07},
			`{"foo":117901063}`,
		},
		{
			"int64",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.int64,
				},
			},
			[]byte{0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07},
			`{"foo":506381209866536711}`,
		},
		{
			"uint8",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.uint8,
				},
			},
			[]byte{0x01},
			`{"foo":1}`,
		},
		{
			"uint16",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.uint16,
				},
			},
			[]byte{0x07, 0x07},
			`{"foo":1799}`,
		},
		{
			"uint32",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.uint32,
				},
			},
			[]byte{0x07, 0x07, 0x07, 0x07},
			`{"foo":117901063}`,
		},
		{
			"uint64",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.uint64,
				},
			},
			[]byte{0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07},
			`{"foo":506381209866536711}`,
		},
		{
			"float32",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.float32,
				},
			},
			[]byte{208, 15, 73, 64},
			`{"foo":3.14159}`,
		},
		{
			"float64",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.float64,
				},
			},
			[]byte{24, 106, 203, 110, 105, 118, 1, 64},
			`{"foo":2.18281828459045}`,
		},
		{
			"time",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.time,
				},
			},
			[]byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00},
			`{"foo":1.000000001}`,
		},
		{
			"time zero",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.time,
				},
			},
			[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			`{"foo":0.000000000}`,
		},
		{
			"duration",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.duration,
				},
			},
			[]byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00},
			`{"foo":1.000000001}`,
		},
		{
			"two fields",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.bool,
				},
				{
					name:      "bar",
					converter: transcoder.bool,
				},
			},
			[]byte{0x01, 0x01},
			`{"foo":true,"bar":true}`,
		},
		{
			"variable-length array",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.array(transcoder.bool, 0, false),
				},
			},
			[]byte{0x02, 0x00, 0x00, 0x00, 0x01, 0x00},
			`{"foo":[true,false]}`,
		},
		{
			"fixed-length array",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.array(transcoder.bool, 2, false),
				},
			},
			[]byte{0x01, 0x00},
			`{"foo":[true,false]}`,
		},
		{
			"byte array",
			"",
			[]recordField{
				{
					name:      "foo",
					converter: transcoder.array(transcoder.uint8, 0, true),
				},
			},
			[]byte{
				0x05, 0x00, 0x00, 0x00,
				'h', 'e', 'l', 'l', 'o',
			},
			`{"foo":"aGVsbG8="}`,
		},
		{
			"array of record",
			"",
			[]recordField{
				{
					name: "foo",
					converter: transcoder.array(
						transcoder.record([]recordField{
							{
								name:      "bar",
								converter: transcoder.bool,
							},
						}),
						0,
						false,
					),
				},
			},
			[]byte{0x01, 0x00, 0x00, 0x00, 0x01},
			`{"foo":[{"bar":true}]}`,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			transcoder.parentPackage = c.parentPackage
			buf := &bytes.Buffer{}
			converter := transcoder.record(c.fields)
			err := converter(buf, bytes.NewBuffer(c.input))
			assert.Nil(t, err)
			assert.Equal(t, c.output, buf.String())
		})
	}
}
