package libmcap

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReadPrefixedBytes(t *testing.T) {
	cases := []struct {
		assertion      string
		data           []byte
		expectedBytes  []byte
		expectedOffset int
		expectedError  error
	}{
		{
			"short length",
			make([]byte, 3),
			nil,
			0,
			io.ErrShortBuffer,
		},
		{
			"short content",
			[]byte{0x01, 0x00, 0x00, 0x00},
			nil,
			0,
			io.ErrShortBuffer,
		},
		{
			"good bytes",
			[]byte{0x05, 0x00, 0x00, 0x00, 'H', 'e', 'l', 'l', 'o'},
			[]byte{'H', 'e', 'l', 'l', 'o'},
			9,
			nil,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			s, off, err := readPrefixedBytes(c.data, 0)
			assert.ErrorIs(t, c.expectedError, err)
			assert.Equal(t, c.expectedBytes, s)
			assert.Equal(t, c.expectedOffset, off)
		})
	}
}

func TestReadPrefixedMap(t *testing.T) {
	cases := []struct {
		assertion string
		input     []byte
		output    map[string]string
		newOffset int
		err       error
	}{
		{
			"short length",
			[]byte{},
			nil,
			0,
			io.ErrShortBuffer,
		},
		{
			"short key",
			flatten(
				encodedUint32(16),
				encodedUint32(4),
				[]byte("foo"),
			),
			nil,
			0,
			io.ErrShortBuffer,
		},
		{
			"short value",
			flatten(
				encodedUint32(16),
				prefixedString("food"),
				encodedUint32(4),
				[]byte("foo"),
			),
			nil,
			0,
			io.ErrShortBuffer,
		},
		{
			"valid map",
			flatten(
				encodedUint32(14),
				prefixedString("foo"),
				prefixedString("bar"),
			),
			map[string]string{
				"foo": "bar",
			},
			18,
			nil,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			output, offset, err := readPrefixedMap(c.input, 0)
			assert.ErrorIs(t, err, c.err)
			assert.Equal(t, offset, c.newOffset)
			assert.Equal(t, output, c.output)
		})
	}
}

func TestReadPrefixedString(t *testing.T) {
	cases := []struct {
		assertion      string
		data           []byte
		expectedString string
		expectedOffset int
		expectedError  error
	}{
		{
			"short length",
			make([]byte, 3),
			"",
			0,
			io.ErrShortBuffer,
		},
		{
			"short content",
			[]byte{0x01, 0x00, 0x00, 0x00},
			"",
			0,
			io.ErrShortBuffer,
		},
		{
			"good string",
			[]byte{0x05, 0x00, 0x00, 0x00, 0x48, 0x65, 0x6c, 0x6c, 0x6f},
			"Hello",
			9,
			nil,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			s, off, err := readPrefixedString(c.data, 0)
			assert.ErrorIs(t, c.expectedError, err)
			assert.Equal(t, c.expectedString, s)
			assert.Equal(t, c.expectedOffset, off)
		})
	}
}

func TestMessageReading(t *testing.T) {
	for _, compression := range []CompressionFormat{
		CompressionNone,
		CompressionZSTD,
		CompressionLZ4,
	} {
		t.Run(fmt.Sprintf("writer compression %s", compression), func(t *testing.T) {
			for _, useIndex := range []bool{
				true,
				false,
			} {
				t.Run(fmt.Sprintf("indexed reading %v", useIndex), func(t *testing.T) {
					buf := &bytes.Buffer{}
					w, err := NewWriter(buf, &WriterOptions{
						Chunked:     true,
						Compression: compression,
						IncludeCRC:  true,
					})
					assert.Nil(t, err)
					err = w.WriteHeader(&Header{
						Profile: "ros1",
					})
					assert.Nil(t, err)
					assert.Nil(t, w.WriteSchema(&Schema{
						ID:       1,
						Name:     "foo",
						Encoding: "msg",
						Data:     []byte{},
					}))
					assert.Nil(t, w.WriteChannel(&Channel{
						ID:              0,
						Topic:           "/test1",
						SchemaID:        1,
						MessageEncoding: "ros1",
					}))
					assert.Nil(t, w.WriteChannel(&Channel{
						ID:              1,
						Topic:           "/test2",
						MessageEncoding: "ros1",
						SchemaID:        1,
					}))
					for i := 0; i < 1000; i++ {
						err := w.WriteMessage(&Message{
							ChannelID:   uint16(i % 2),
							Sequence:    0,
							LogTime:     uint64(i),
							PublishTime: uint64(i),
							Data:        []byte{1, 2, 3, 4},
						})
						assert.Nil(t, err)
					}
					w.Close()
					t.Run("read all messages", func(t *testing.T) {
						reader := bytes.NewReader(buf.Bytes())
						r, err := NewReader(reader)
						assert.Nil(t, err)
						it, err := r.Messages(0, 10000, []string{}, useIndex)
						assert.Nil(t, err)
						c := 0
						for {
							ci, msg, err := it.Next(nil)
							if errors.Is(err, io.EOF) {
								break
							}
							assert.Nil(t, err)
							assert.NotNil(t, ci)
							assert.NotNil(t, msg)
							assert.Equal(t, msg.ChannelID, ci.ID)
							c++
						}
						assert.Equal(t, 1000, c)
					})
					t.Run("read messages on one topic", func(t *testing.T) {
						reader := bytes.NewReader(buf.Bytes())
						r, err := NewReader(reader)
						assert.Nil(t, err)
						it, err := r.Messages(0, 10000, []string{"/test1"}, useIndex)
						assert.Nil(t, err)
						c := 0
						for {
							ci, msg, err := it.Next(nil)
							if errors.Is(err, io.EOF) {
								break
							}
							assert.Nil(t, err)
							assert.NotNil(t, ci)
							assert.NotNil(t, msg)
							assert.Equal(t, msg.ChannelID, ci.ID)
							c++
						}
						assert.Equal(t, 500, c)
					})
					t.Run("read messages on multiple topics", func(t *testing.T) {
						reader := bytes.NewReader(buf.Bytes())
						r, err := NewReader(reader)
						assert.Nil(t, err)
						it, err := r.Messages(0, 10000, []string{"/test1", "/test2"}, useIndex)
						assert.Nil(t, err)
						c := 0
						for {
							ci, msg, err := it.Next(nil)
							if errors.Is(err, io.EOF) {
								break
							}
							assert.Nil(t, err)
							assert.NotNil(t, ci)
							assert.NotNil(t, msg)
							assert.Equal(t, msg.ChannelID, ci.ID)
							c++
						}
						assert.Equal(t, 1000, c)
					})
					t.Run("read messages in time range", func(t *testing.T) {
						reader := bytes.NewReader(buf.Bytes())
						r, err := NewReader(reader)
						assert.Nil(t, err)
						it, err := r.Messages(100, 200, []string{}, useIndex)
						assert.Nil(t, err)
						c := 0
						for {
							_, _, err := it.Next(nil)
							if errors.Is(err, io.EOF) {
								break
							}
							assert.Nil(t, err)
							c++
						}
						assert.Equal(t, 100, c)
					})
				})
			}
		})
	}
}

func TestReaderCounting(t *testing.T) {
	for _, indexed := range []bool{
		true,
		false,
	} {
		t.Run(fmt.Sprintf("indexed %v", indexed), func(t *testing.T) {
			f, err := os.Open("../../testdata/mcap/demo.mcap")
			assert.Nil(t, err)
			defer f.Close()
			r, err := NewReader(f)
			assert.Nil(t, err)
			it, err := r.Messages(0, time.Now().UnixNano(), []string{}, indexed)
			assert.Nil(t, err)
			c := 0
			for {
				_, _, err := it.Next(nil)
				if errors.Is(err, io.EOF) {
					break
				}
				assert.Nil(t, err)
				c++
			}
			assert.Equal(t, 1606, c)
		})
	}
}

func TestMCAPInfo(t *testing.T) {
	f, err := os.Open("../../testdata/mcap/demo.mcap")
	assert.Nil(t, err)
	defer f.Close()
	assert.Nil(t, err)
	r, err := NewReader(f)
	assert.Nil(t, err)
	info, err := r.Info()
	assert.Nil(t, err)
	assert.Equal(t, uint64(1606), info.Statistics.MessageCount)
	assert.Equal(t, uint32(7), info.Statistics.ChannelCount)
	assert.Equal(t, uint32(27), info.Statistics.ChunkCount)
	expectedCounts := map[string]uint64{
		"/radar/points":           156,
		"/radar/tracks":           156,
		"/radar/range":            156,
		"/velodyne_points":        78,
		"/diagnostics":            52,
		"/tf":                     774,
		"/image_color/compressed": 234,
	}
	for k, v := range info.ChannelCounts() {
		assert.Equal(t, expectedCounts[k], v, "mismatch on %s - got %d", k, v)
	}
}

func TestReadingDiagnostics(t *testing.T) {
	f, err := os.Open("../../testdata/mcap/demo.mcap")
	assert.Nil(t, err)
	defer f.Close()
	assert.Nil(t, err)
	r, err := NewReader(f)
	assert.Nil(t, err)
	it, err := r.Messages(0, time.Now().UnixNano(), []string{"/diagnostics"}, true)
	assert.Nil(t, err)
	c := 0
	for {
		_, _, err := it.Next(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		assert.Nil(t, err)
		c++
	}
	assert.Equal(t, 52, c)
}
