package mcap

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexedReaderBreaksTiesOnChunkOffset(t *testing.T) {
	buf := &bytes.Buffer{}
	writer, err := NewWriter(buf, &WriterOptions{
		Chunked:   true,
		ChunkSize: 10000,
	})
	require.NoError(t, err)
	require.NoError(t, writer.WriteHeader(&Header{}))
	require.NoError(t, writer.WriteSchema(&Schema{
		ID:       1,
		Name:     "",
		Encoding: "",
		Data:     []byte{},
	}))
	require.NoError(t, writer.WriteChannel(&Channel{
		ID:              0,
		SchemaID:        0,
		Topic:           "/foo",
		MessageEncoding: "",
		Metadata: map[string]string{
			"": "",
		},
	}))
	require.NoError(t, writer.WriteChannel(&Channel{
		ID:              1,
		SchemaID:        0,
		Topic:           "/bar",
		MessageEncoding: "",
		Metadata: map[string]string{
			"": "",
		},
	}))
	require.NoError(t, writer.WriteMessage(&Message{
		ChannelID:   0,
		Sequence:    0,
		LogTime:     0,
		PublishTime: 0,
		Data:        []byte{'h', 'e', 'l', 'l', 'o'},
	}))
	require.NoError(t, writer.WriteMessage(&Message{
		ChannelID:   1,
		Sequence:    0,
		LogTime:     0,
		PublishTime: 0,
		Data:        []byte{'g', 'o', 'o', 'd', 'b', 'y', 'e'},
	}))
	writer.Close()

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	it, err := reader.Messages(UsingIndex(true))
	require.NoError(t, err)
	expectedTopics := []string{"/foo", "/bar"}
	for i := 0; i < 2; i++ {
		_, channel, _, err := it.Next(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		assert.Equal(t, expectedTopics[i], channel.Topic)
	}
}
func TestReaderFallsBackToLinearScan(t *testing.T) {
	buf := &bytes.Buffer{}
	writer, err := NewWriter(buf, &WriterOptions{
		Chunked: false,
	})
	require.NoError(t, err)
	require.NoError(t, writer.WriteHeader(&Header{}))
	require.NoError(t, writer.WriteSchema(&Schema{
		ID:       1,
		Name:     "",
		Encoding: "",
		Data:     []byte{},
	}))
	require.NoError(t, writer.WriteChannel(&Channel{
		ID:              0,
		SchemaID:        1,
		Topic:           "/foo",
		MessageEncoding: "",
		Metadata: map[string]string{
			"": "",
		},
	}))
	require.NoError(t, writer.WriteMessage(&Message{
		ChannelID:   0,
		Sequence:    0,
		LogTime:     0,
		PublishTime: 0,
		Data:        []byte("hello"),
	}))
	require.NoError(t, writer.WriteMessage(&Message{
		ChannelID:   0,
		Sequence:    1,
		LogTime:     1,
		PublishTime: 1,
		Data:        []byte("goodbye"),
	}))
	writer.Close()

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	it, err := reader.Messages(UsingIndex(true))
	messageContents := []string{"hello", "goodbye"}
	require.NoError(t, err)
	for _, content := range messageContents {
		_, _, msg, err := it.Next(nil)
		require.NoError(t, err)
		assert.Equal(t, content, string(msg.Data))
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
			require.ErrorIs(t, c.expectedError, err)
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
			require.ErrorIs(t, err, c.err)
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
			require.ErrorIs(t, c.expectedError, err)
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
					require.NoError(t, err)
					err = w.WriteHeader(&Header{
						Profile: "ros1",
					})
					require.NoError(t, err)
					require.NoError(t, w.WriteSchema(&Schema{
						ID:       1,
						Name:     "foo",
						Encoding: "msg",
						Data:     []byte{},
					}))
					require.NoError(t, w.WriteChannel(&Channel{
						ID:              0,
						Topic:           "/test1",
						SchemaID:        1,
						MessageEncoding: "ros1",
					}))
					require.NoError(t, w.WriteChannel(&Channel{
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
						require.NoError(t, err)
					}
					w.Close()
					t.Run("read all messages", func(t *testing.T) {
						reader := bytes.NewReader(buf.Bytes())
						r, err := NewReader(reader)
						require.NoError(t, err)
						it, err := r.Messages(UsingIndex(useIndex))
						require.NoError(t, err)
						c := 0
						for {
							schema, channel, message, err := it.Next(nil)
							if errors.Is(err, io.EOF) {
								break
							}
							require.NoError(t, err)
							require.NotNil(t, channel)
							require.NotNil(t, message)
							assert.Equal(t, message.ChannelID, channel.ID)
							require.NotNil(t, schema)
							assert.Equal(t, schema.ID, channel.SchemaID)
							c++
						}
						assert.Equal(t, 1000, c)
					})
					t.Run("read messages on one topic", func(t *testing.T) {
						reader := bytes.NewReader(buf.Bytes())
						r, err := NewReader(reader)
						require.NoError(t, err)
						it, err := r.Messages(
							WithTopics([]string{"/test1"}),
							UsingIndex(useIndex),
						)
						require.NoError(t, err)
						c := 0
						for {
							schema, channel, message, err := it.Next(nil)
							if errors.Is(err, io.EOF) {
								break
							}
							require.NoError(t, err)
							require.NotNil(t, channel)
							require.NotNil(t, message)
							require.NotNil(t, schema)
							assert.Equal(t, message.ChannelID, channel.ID)
							assert.Equal(t, schema.ID, channel.SchemaID)
							c++
						}
						assert.Equal(t, 500, c)
					})
					t.Run("read messages on multiple topics", func(t *testing.T) {
						reader := bytes.NewReader(buf.Bytes())
						r, err := NewReader(reader)
						require.NoError(t, err)
						it, err := r.Messages(
							WithTopics([]string{"/test1", "/test2"}),
							UsingIndex(useIndex),
						)
						require.NoError(t, err)
						c := 0
						for {
							schema, channel, message, err := it.Next(nil)
							if errors.Is(err, io.EOF) {
								break
							}
							require.NoError(t, err)
							require.NotNil(t, channel)
							require.NotNil(t, message)
							require.NotNil(t, schema)
							assert.Equal(t, message.ChannelID, channel.ID)
							assert.Equal(t, channel.SchemaID, schema.ID)
							c++
						}
						assert.Equal(t, 1000, c)
					})
					t.Run("read messages in time range", func(t *testing.T) {
						reader := bytes.NewReader(buf.Bytes())
						r, err := NewReader(reader)
						require.NoError(t, err)
						it, err := r.Messages(
							AfterNanos(100),
							BeforeNanos(200),
							UsingIndex(useIndex),
						)
						require.NoError(t, err)
						c := 0
						for {
							_, _, _, err := it.Next(nil)
							if errors.Is(err, io.EOF) {
								break
							}
							require.NoError(t, err)
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
			require.NoError(t, err)
			defer f.Close()
			r, err := NewReader(f)
			require.NoError(t, err)
			it, err := r.Messages(UsingIndex(indexed))
			require.NoError(t, err)
			c := 0
			for {
				_, _, _, err := it.Next(nil)
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
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
			require.NoError(t, err)
			require.NoError(t, w.WriteHeader(&Header{}))
			for _, schema := range c.schemas {
				require.NoError(t, w.WriteSchema(schema))
			}
			for _, channel := range c.channels {
				require.NoError(t, w.WriteChannel(channel))
			}
			for _, message := range c.messages {
				require.NoError(t, w.WriteMessage(message))
			}
			for _, metadata := range c.metadata {
				require.NoError(t, w.WriteMetadata(metadata))
			}
			for _, attachment := range c.attachments {
				require.NoError(t, w.WriteAttachment(attachment))
			}
			require.NoError(t, w.Close())

			reader := bytes.NewReader(buf.Bytes())
			r, err := NewReader(reader)
			require.NoError(t, err)
			info, err := r.Info()
			require.NoError(t, err)
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
				require.NoError(t, err)
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

func TestReaderMetadataCallback(t *testing.T) {
	cases := []struct {
		assertion string
		useIndex  bool
	}{
		{
			"using index",
			true,
		},
		{
			"without index",
			false,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf := &bytes.Buffer{}
			writer, err := NewWriter(buf, &WriterOptions{
				IncludeCRC: false,
				Chunked:    true,
				ChunkSize:  1024,
			})
			require.NoError(t, err)
			require.NoError(t, writer.WriteHeader(&Header{}))
			require.NoError(t, writer.WriteMetadata(&Metadata{
				Name:     "foo",
				Metadata: map[string]string{"foo": "bar"},
			}))
			require.NoError(t, writer.Close())
			data := bytes.NewReader(buf.Bytes())
			reader, err := NewReader(data)
			require.NoError(t, err)

			var recordName string
			it, err := reader.Messages(UsingIndex(c.useIndex), WithMetadataCallback(func(m *Metadata) error {
				recordName = m.Name
				return nil
			}))
			require.NoError(t, err)
			_, _, _, err = it.Next(nil)
			require.ErrorIs(t, err, io.EOF)

			assert.Equal(t, "foo", recordName)
		})
	}
}

func TestReadingDiagnostics(t *testing.T) {
	f, err := os.Open("../../testdata/mcap/demo.mcap")
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, err)
	r, err := NewReader(f)
	require.NoError(t, err)
	it, err := r.Messages(WithTopics([]string{"/diagnostics"}))
	require.NoError(t, err)
	c := 0
	for {
		_, _, _, err := it.Next(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		c++
	}
	assert.Equal(t, 52, c)
}

func TestReadingMetadata(t *testing.T) {
	buf := &bytes.Buffer{}
	writer, err := NewWriter(buf, &WriterOptions{
		Chunked:     true,
		ChunkSize:   1024,
		Compression: "",
	})
	require.NoError(t, err)
	require.NoError(t, writer.WriteHeader(&Header{}))

	expectedMetadata := &Metadata{
		Name: "foo",
		Metadata: map[string]string{
			"foo": "bar",
		},
	}
	require.NoError(t, writer.WriteMetadata(expectedMetadata))
	require.NoError(t, writer.Close())

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	info, err := reader.Info()
	require.NoError(t, err)
	assert.Len(t, info.MetadataIndexes, 1)
	idx := info.MetadataIndexes[0]
	metadata, err := reader.GetMetadata(idx.Offset)
	require.NoError(t, err)
	assert.Equal(t, expectedMetadata, metadata)
}

func TestGetAttachmentReader(t *testing.T) {
	buf := &bytes.Buffer{}
	writer, err := NewWriter(buf, &WriterOptions{
		Chunked:     true,
		ChunkSize:   1024,
		Compression: "",
	})
	require.NoError(t, err)
	require.NoError(t, writer.WriteHeader(&Header{}))
	require.NoError(t, writer.WriteAttachment(&Attachment{
		LogTime:    10,
		CreateTime: 1000,
		Name:       "foo",
		MediaType:  "text",
		DataSize:   3,
		Data:       bytes.NewReader([]byte{'a', 'b', 'c'}),
	}))
	require.NoError(t, writer.Close())

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	info, err := reader.Info()
	require.NoError(t, err)
	assert.Len(t, info.AttachmentIndexes, 1)
	idx := info.AttachmentIndexes[0]
	ar, err := reader.GetAttachmentReader(idx.Offset)
	require.NoError(t, err)

	assert.Equal(t, "foo", ar.Name)
	assert.Equal(t, "text", ar.MediaType)
	assert.Equal(t, 3, int(ar.DataSize))
	assert.Equal(t, 10, int(ar.LogTime))
	assert.Equal(t, 1000, int(ar.CreateTime))

	data, err := io.ReadAll(ar.Data())
	require.NoError(t, err)
	assert.Equal(t, []byte{'a', 'b', 'c'}, data)
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
	require.NoError(t, err)
	require.NoError(t, writer.WriteHeader(&Header{}))
	require.NoError(t, writer.WriteSchema(&Schema{
		ID:       1,
		Name:     "",
		Encoding: "",
		Data:     []byte{},
	}))
	require.NoError(t, writer.WriteChannel(&Channel{
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
		require.NoError(t, writer.WriteMessage(&Message{
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
	require.NoError(t, writer.Close())

	// start reading the MCAP back
	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	it, err := reader.Messages(
		UsingIndex(true),
		InOrder(LogTimeOrder),
	)
	require.NoError(t, err)

	// check that timestamps monotonically increase from the returned iterator
	var lastSeenTimestamp uint64
	for i := 0; i < msgCount; i++ {
		_, _, msg, err := it.Next(nil)
		require.NoError(t, err)
		if i != 0 {
			assert.Greater(t, msg.LogTime, lastSeenTimestamp)
		}
		lastSeenTimestamp = msg.LogTime
	}
	_, _, msg, err := it.Next(nil)
	require.Nil(t, msg)
	require.ErrorIs(t, io.EOF, err)

	// now try iterating in reverse
	reverseIt, err := reader.Messages(
		UsingIndex(true),
		InOrder(ReverseLogTimeOrder),
	)
	require.NoError(t, err)

	// check that timestamps monotonically decrease from the returned iterator
	for i := 0; i < msgCount; i++ {
		_, _, msg, err := reverseIt.Next(nil)
		require.NoError(t, err)
		if i != 0 {
			assert.Less(t, msg.LogTime, lastSeenTimestamp)
		}
		lastSeenTimestamp = msg.LogTime
	}
	_, _, msg, err = reverseIt.Next(nil)
	require.Nil(t, msg)
	require.ErrorIs(t, io.EOF, err)
}

func TestReadingBigTimestamps(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewWriter(buf, &WriterOptions{
		Chunked:   true,
		ChunkSize: 100,
	})
	require.NoError(t, err)
	require.NoError(t, w.WriteHeader(&Header{}))
	require.NoError(t, w.WriteSchema(&Schema{ID: 1}))
	require.NoError(t, w.WriteChannel(&Channel{SchemaID: 1, Topic: "/topic"}))
	require.NoError(t, w.WriteMessage(&Message{
		LogTime: math.MaxUint64 - 1,
		Data:    []byte("hello"),
	}))
	require.NoError(t, w.Close())
	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	t.Run("info works as expected", func(t *testing.T) {
		info, err := reader.Info()
		require.NoError(t, err)
		assert.Equal(t, uint64(math.MaxUint64-1), info.Statistics.MessageEndTime)
	})
	t.Run("message iteration works as expected", func(t *testing.T) {
		it, err := reader.Messages(AfterNanos(math.MaxUint64-2), BeforeNanos(math.MaxUint64))
		require.NoError(t, err)
		count := 0
		for {
			_, _, msg, err := it.Next(nil)
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			assert.Equal(t, []byte("hello"), msg.Data)
			count++
		}
		assert.Equal(t, 1, count)
	})
}

func BenchmarkReader(b *testing.B) {
	inputParameters := []struct {
		name                   string
		outOfOrderWithinChunks bool
		chunksOverlap          bool
	}{
		{
			name: "inorder",
		},
		{
			name:                   "minor",
			outOfOrderWithinChunks: true,
		},
		{
			name:                   "major",
			outOfOrderWithinChunks: true,
			chunksOverlap:          true,
		},
	}
	for _, inputCfg := range inputParameters {
		b.Run(inputCfg.name, func(b *testing.B) {
			b.StopTimer()
			buf := &bytes.Buffer{}
			writer, err := NewWriter(buf, &WriterOptions{
				Chunked:     true,
				Compression: CompressionZSTD,
			})
			require.NoError(b, err)
			messageCount := uint64(1000000)
			require.NoError(b, writer.WriteHeader(&Header{}))
			require.NoError(b, writer.WriteSchema(&Schema{ID: 1, Name: "empty", Encoding: "none"}))
			channelCount := 200
			for i := 0; i < channelCount; i++ {
				require.NoError(b, writer.WriteChannel(&Channel{
					ID:              uint16(i),
					SchemaID:        1,
					Topic:           "/chat",
					MessageEncoding: "none",
				}))
			}
			contentBuf := make([]byte, 32)
			lastChunkMax := uint64(0)
			thisChunkMax := uint64(0)
			for i := uint64(0); i < messageCount; i++ {
				channelID := uint16(i % uint64(channelCount))
				_, err := rand.Read(contentBuf)
				require.NoError(b, err)
				timestamp := i
				if inputCfg.outOfOrderWithinChunks {
					timestamp += (2 * (10 - (i % 10)))
					if !inputCfg.chunksOverlap {
						if timestamp < lastChunkMax {
							timestamp = lastChunkMax
						}
					}
				}
				if timestamp > thisChunkMax {
					thisChunkMax = timestamp
				}
				chunkCount := len(writer.ChunkIndexes)
				require.NoError(b, writer.WriteMessage(&Message{
					ChannelID:   channelID,
					Sequence:    uint32(i),
					LogTime:     timestamp,
					PublishTime: timestamp,
					Data:        contentBuf,
				}))
				if len(writer.ChunkIndexes) != chunkCount {
					lastChunkMax = thisChunkMax
				}
			}
			require.NoError(b, writer.Close())
			b.StartTimer()
			readerConfigs := []struct {
				opts []ReadOpt
				name string
			}{
				{
					opts: []ReadOpt{
						UsingIndex(false),
					},
					name: "no_index",
				},
				{
					opts: []ReadOpt{
						UsingIndex(true),
						InOrder(FileOrder),
					},
					name: "index_file_order",
				},
				{
					opts: []ReadOpt{
						UsingIndex(true),
						InOrder(LogTimeOrder),
					},
					name: "index_time_order",
				},
				{
					opts: []ReadOpt{
						UsingIndex(true),
						InOrder(ReverseLogTimeOrder),
					},
					name: "index_rev_order",
				},
			}
			for _, cfg := range readerConfigs {
				b.Run(cfg.name, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						s := time.Now()
						reader, err := NewReader(bytes.NewReader(buf.Bytes()))
						require.NoError(b, err)
						it, err := reader.Messages(cfg.opts...)
						require.NoError(b, err)
						readMessages := uint64(0)
						msgBytes := uint64(0)
						msg := Message{}
						for {
							_, _, msg, err := it.Next2(&msg)
							if errors.Is(err, io.EOF) {
								break
							}
							require.NoError(b, err)
							readMessages++
							msgBytes += uint64(len(msg.Data))
						}
						b.ReportMetric(float64(messageCount)/time.Since(s).Seconds(), "msg/s")
						b.ReportMetric(float64(msgBytes)/(time.Since(s).Seconds()*1024*1024), "MB/s")
						require.Equal(b, messageCount, readMessages)
					}
				})
			}
			b.Run("bare_lexer", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					s := time.Now()
					lexer, err := NewLexer(bytes.NewReader(buf.Bytes()))
					require.NoError(b, err)
					readMessages := uint64(0)
					msgBytes := uint64(0)
					var p []byte
					for {
						token, record, err := lexer.Next(p)
						if errors.Is(err, io.EOF) {
							break
						}
						require.NoError(b, err)
						if cap(record) > cap(p) {
							p = record
						}
						if token == TokenMessage {
							readMessages++
							msgBytes += uint64(len(record) - 22)
						}
					}
					b.ReportMetric(float64(messageCount)/time.Since(s).Seconds(), "msg/s")
					b.ReportMetric(float64(msgBytes)/(time.Since(s).Seconds()*1024*1024), "MB/s")
					require.Equal(b, messageCount, readMessages)
				}
			})
		})
	}
}
