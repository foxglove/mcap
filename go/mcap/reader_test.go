package mcap

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/foxglove/mcap/go/mcap/readopts"
	"github.com/stretchr/testify/assert"
)

func TestIndexedReaderBreaksTiesOnChunkOffset(t *testing.T) {
	buf := &bytes.Buffer{}
	writer, err := NewWriter(buf, &WriterOptions{
		Chunked:   true,
		ChunkSize: 10000,
	})
	assert.Nil(t, err)
	assert.Nil(t, writer.WriteHeader(&Header{}))
	assert.Nil(t, writer.WriteSchema(&Schema{
		ID:       0,
		Name:     "",
		Encoding: "",
		Data:     []byte{},
	}))
	assert.Nil(t, writer.WriteChannel(&Channel{
		ID:              0,
		SchemaID:        0,
		Topic:           "/foo",
		MessageEncoding: "",
		Metadata: map[string]string{
			"": "",
		},
	}))
	assert.Nil(t, writer.WriteChannel(&Channel{
		ID:              1,
		SchemaID:        0,
		Topic:           "/bar",
		MessageEncoding: "",
		Metadata: map[string]string{
			"": "",
		},
	}))
	assert.Nil(t, writer.WriteMessage(&Message{
		ChannelID:   0,
		Sequence:    0,
		LogTime:     0,
		PublishTime: 0,
		Data:        []byte{'h', 'e', 'l', 'l', 'o'},
	}))
	assert.Nil(t, writer.WriteMessage(&Message{
		ChannelID:   1,
		Sequence:    0,
		LogTime:     0,
		PublishTime: 0,
		Data:        []byte{'g', 'o', 'o', 'd', 'b', 'y', 'e'},
	}))
	writer.Close()

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	assert.Nil(t, err)

	it, err := reader.Messages(readopts.UsingIndex(true))
	assert.Nil(t, err)
	expectedTopics := []string{"/foo", "/bar"}
	for i := 0; i < 2; i++ {
		_, channel, _, err := it.Next(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		assert.Equal(t, expectedTopics[i], channel.Topic)
	}
}

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
			s, off, err := getPrefixedBytes(c.data, 0)
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
			output, offset, err := getPrefixedMap(c.input, 0)
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
			s, off, err := getPrefixedString(c.data, 0)
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
						it, err := r.Messages(readopts.UsingIndex(useIndex))
						assert.Nil(t, err)
						c := 0
						for {
							schema, channel, message, err := it.Next(nil)
							if errors.Is(err, io.EOF) {
								break
							}
							assert.Nil(t, err)
							assert.NotNil(t, channel)
							assert.NotNil(t, message)
							assert.Equal(t, message.ChannelID, channel.ID)
							assert.NotNil(t, schema)
							assert.Equal(t, schema.ID, channel.SchemaID)
							c++
						}
						assert.Equal(t, 1000, c)
					})
					t.Run("read messages on one topic", func(t *testing.T) {
						reader := bytes.NewReader(buf.Bytes())
						r, err := NewReader(reader)
						assert.Nil(t, err)
						it, err := r.Messages(
							readopts.WithTopics([]string{"/test1"}),
							readopts.UsingIndex(useIndex),
						)
						assert.Nil(t, err)
						c := 0
						for {
							schema, channel, message, err := it.Next(nil)
							if errors.Is(err, io.EOF) {
								break
							}
							assert.Nil(t, err)
							assert.NotNil(t, channel)
							assert.NotNil(t, message)
							assert.NotNil(t, schema)
							assert.Equal(t, message.ChannelID, channel.ID)
							assert.Equal(t, schema.ID, channel.SchemaID)
							c++
						}
						assert.Equal(t, 500, c)
					})
					t.Run("read messages on multiple topics", func(t *testing.T) {
						reader := bytes.NewReader(buf.Bytes())
						r, err := NewReader(reader)
						assert.Nil(t, err)
						it, err := r.Messages(
							readopts.WithTopics([]string{"/test1", "/test2"}),
							readopts.UsingIndex(useIndex),
						)
						assert.Nil(t, err)
						c := 0
						for {
							schema, channel, message, err := it.Next(nil)
							if errors.Is(err, io.EOF) {
								break
							}
							assert.Nil(t, err)
							assert.NotNil(t, channel)
							assert.NotNil(t, message)
							assert.NotNil(t, schema)
							assert.Equal(t, message.ChannelID, channel.ID)
							assert.Equal(t, channel.SchemaID, schema.ID)
							c++
						}
						assert.Equal(t, 1000, c)
					})
					t.Run("read messages in time range", func(t *testing.T) {
						reader := bytes.NewReader(buf.Bytes())
						r, err := NewReader(reader)
						assert.Nil(t, err)
						it, err := r.Messages(
							readopts.After(100),
							readopts.Before(200),
							readopts.UsingIndex(useIndex),
						)
						assert.Nil(t, err)
						c := 0
						for {
							_, _, _, err := it.Next(nil)
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
			it, err := r.Messages(readopts.UsingIndex(indexed))
			assert.Nil(t, err)
			c := 0
			for {
				_, _, _, err := it.Next(nil)
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
	cases := []struct {
		assertion   string
		schemas     []*Schema
		channels    []*Channel
		messages    []*Message
		metadata    []*Metadata
		attachments []*Attachment
	}{
		{
			"no metadata or attachments",
			[]*Schema{
				{
					ID: 1,
				},
				{
					ID: 2,
				},
			},
			[]*Channel{
				{
					ID:       1,
					SchemaID: 1,
					Topic:    "/foo",
				},
				{
					ID:       2,
					SchemaID: 2,
					Topic:    "/bar",
				},
			},
			[]*Message{
				{
					ChannelID: 1,
				},
				{
					ChannelID: 2,
				},
			},
			[]*Metadata{},
			[]*Attachment{},
		},
		{
			"no metadata or attachments",
			[]*Schema{
				{
					ID: 1,
				},
				{
					ID: 2,
				},
			},
			[]*Channel{
				{
					ID:       1,
					SchemaID: 1,
					Topic:    "/foo",
				},
				{
					ID:       2,
					SchemaID: 2,
					Topic:    "/bar",
				},
			},
			[]*Message{
				{
					ChannelID: 1,
				},
				{
					ChannelID: 2,
				},
			},
			[]*Metadata{
				{
					Name: "metadata1",
					Metadata: map[string]string{
						"foo": "bar",
					},
				},
				{
					Name: "metadata2",
					Metadata: map[string]string{
						"foo": "bar",
					},
				},
			},
			[]*Attachment{
				{
					Name: "my attachment",
					Data: &bytes.Buffer{},
				},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w, err := NewWriter(buf, &WriterOptions{
				Chunked:     true,
				ChunkSize:   1024,
				Compression: CompressionLZ4,
			})
			assert.Nil(t, err)
			assert.Nil(t, w.WriteHeader(&Header{}))
			for _, schema := range c.schemas {
				assert.Nil(t, w.WriteSchema(schema))
			}
			for _, channel := range c.channels {
				assert.Nil(t, w.WriteChannel(channel))
			}
			for _, message := range c.messages {
				assert.Nil(t, w.WriteMessage(message))
			}
			for _, metadata := range c.metadata {
				assert.Nil(t, w.WriteMetadata(metadata))
			}
			for _, attachment := range c.attachments {
				assert.Nil(t, w.WriteAttachment(attachment))
			}
			assert.Nil(t, w.Close())

			reader := bytes.NewReader(buf.Bytes())
			r, err := NewReader(reader)
			assert.Nil(t, err)
			info, err := r.Info()
			assert.Nil(t, err)
			assert.Equal(t, uint64(len(c.messages)), info.Statistics.MessageCount, "unexpected message count")
			assert.Equal(t, uint32(len(c.channels)), info.Statistics.ChannelCount, "unexpected channel count")
			assert.Equal(t, uint32(len(c.metadata)), info.Statistics.MetadataCount, "unexpected metadata count")
			assert.Equal(
				t,
				uint32(len(c.attachments)),
				info.Statistics.AttachmentCount,
				"unexpected attachment count",
			)
			expectedTopicCounts := make(map[string]uint64)
			for _, message := range c.messages {
				channel, err := find(c.channels, func(channel *Channel) bool {
					return channel.ID == message.ChannelID
				})
				assert.Nil(t, err)
				expectedTopicCounts[channel.Topic]++
			}
			assert.Equal(t, expectedTopicCounts, info.ChannelCounts())
		})
	}
}

// find returns the first element in items that satisfies the given predicate.
func find[T any](items []T, f func(T) bool) (val T, err error) {
	for _, v := range items {
		if f(v) {
			return v, nil
		}
	}
	return val, fmt.Errorf("not found")
}

func TestReadingDiagnostics(t *testing.T) {
	f, err := os.Open("../../testdata/mcap/demo.mcap")
	assert.Nil(t, err)
	defer f.Close()
	assert.Nil(t, err)
	r, err := NewReader(f)
	assert.Nil(t, err)
	it, err := r.Messages(readopts.WithTopics([]string{"/diagnostics"}))
	assert.Nil(t, err)
	c := 0
	for {
		_, _, _, err := it.Next(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		assert.Nil(t, err)
		c++
	}
	assert.Equal(t, 52, c)
}

func TestReadingMessageOrderWithOverlappingChunks(t *testing.T) {
	buf := &bytes.Buffer{}
	// write an MCAP with two chunks, where in each chunk all messages have ascending timestamps,
	// but their timestamp ranges overlap.
	writer, err := NewWriter(buf, &WriterOptions{
		Chunked:     true,
		ChunkSize:   200,
		Compression: CompressionLZ4,
	})
	assert.Nil(t, err)
	assert.Nil(t, writer.WriteHeader(&Header{}))
	assert.Nil(t, writer.WriteSchema(&Schema{
		ID:       0,
		Name:     "",
		Encoding: "",
		Data:     []byte{},
	}))
	assert.Nil(t, writer.WriteChannel(&Channel{
		ID:              0,
		Topic:           "",
		SchemaID:        0,
		MessageEncoding: "",
		Metadata: map[string]string{
			"": "",
		},
	}))
	msgCount := 0
	addMsg := func(timestamp uint64) {
		assert.Nil(t, writer.WriteMessage(&Message{
			ChannelID:   0,
			Sequence:    0,
			LogTime:     timestamp,
			PublishTime: timestamp,
			Data:        []byte{'h', 'e', 'l', 'l', 'o'},
		}))
		msgCount++
	}
	var now uint64 = 100
	addMsg(now)
	for writer.compressedWriter.Size() != 0 {
		now += 10
		addMsg(now)
	}
	// ensure that the chunk contains more than one message
	assert.Greater(t, now, uint64(110))
	// add time discontinuity between chunks
	now -= 55

	addMsg(now)
	for writer.compressedWriter.Size() != 0 {
		now += 10
		addMsg(now)
	}
	assert.Nil(t, writer.Close())

	// start reading the MCAP back
	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	assert.Nil(t, err)

	it, err := reader.Messages(
		readopts.UsingIndex(true),
		readopts.InOrder(readopts.LogTimeOrder),
	)
	assert.Nil(t, err)

	// check that timestamps monotonically increase from the returned iterator
	var lastSeenTimestamp uint64
	for i := 0; i < msgCount; i++ {
		_, _, msg, err := it.Next(nil)
		assert.Nil(t, err)
		if i != 0 {
			assert.Greater(t, msg.LogTime, lastSeenTimestamp)
		}
		lastSeenTimestamp = msg.LogTime
	}
	_, _, msg, err := it.Next(nil)
	assert.Nil(t, msg)
	assert.Error(t, io.EOF, err)

	// now try iterating in reverse
	reverseIt, err := reader.Messages(
		readopts.UsingIndex(true),
		readopts.InOrder(readopts.ReverseLogTimeOrder),
	)
	assert.Nil(t, err)

	// check that timestamps monotonically decrease from the returned iterator
	for i := 0; i < msgCount; i++ {
		_, _, msg, err := reverseIt.Next(nil)
		assert.Nil(t, err)
		if i != 0 {
			assert.Less(t, msg.LogTime, lastSeenTimestamp)
		}
		lastSeenTimestamp = msg.LogTime
	}
	_, _, msg, err = reverseIt.Next(nil)
	assert.Nil(t, msg)
	assert.Error(t, io.EOF, err)
}
