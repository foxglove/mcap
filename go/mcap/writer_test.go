package mcap

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const libraryString = "libfoo v0"

func TestMCAPReadWrite(t *testing.T) {
	t.Run("test header", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w, err := NewWriter(buf, &WriterOptions{Compression: CompressionZSTD, OverrideLibrary: true})
		require.NoError(t, err)
		err = w.WriteHeader(&Header{
			Profile: "ros1",
			Library: libraryString,
		})
		require.NoError(t, err)
		lexer, err := NewLexer(buf)
		require.NoError(t, err)
		defer lexer.Close()
		tokenType, record, err := lexer.Next(nil)
		require.NoError(t, err)
		// body of the header is the profile, followed by the metadata map
		offset := 0
		profile, offset, err := getPrefixedString(record, offset)
		require.NoError(t, err)
		assert.Equal(t, "ros1", profile)
		library, _, err := getPrefixedString(record, offset)
		require.NoError(t, err)
		assert.Equal(t, "libfoo v0", library)
		assert.Equal(t, TokenHeader, tokenType)
	})
	t.Run("zero-valued schema IDs permitted", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w, err := NewWriter(buf, &WriterOptions{Compression: CompressionLZ4})
		require.NoError(t, err)
		err = w.WriteChannel(&Channel{
			ID:              0,
			SchemaID:        0,
			Topic:           "/foo",
			MessageEncoding: "msg",
			Metadata: map[string]string{
				"key": "val",
			},
		})
		require.NoError(t, err)
	})
	t.Run("positive schema IDs rejected if schema unknown", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w, err := NewWriter(buf, &WriterOptions{Compression: CompressionLZ4})
		require.NoError(t, err)
		err = w.WriteChannel(&Channel{
			ID:              0,
			SchemaID:        1,
			Topic:           "/foo",
			MessageEncoding: "msg",
			Metadata: map[string]string{
				"key": "val",
			},
		})
		require.ErrorIs(t, err, ErrUnknownSchema)
	})
}

func TestOutputDeterminism(t *testing.T) {
	var hash string
	for i := 0; i < 10; i++ {
		buf := &bytes.Buffer{}
		w, err := NewWriter(buf, &WriterOptions{
			Chunked:         true,
			Compression:     CompressionZSTD,
			IncludeCRC:      true,
			ChunkSize:       1024,
			OverrideLibrary: true,
		})
		require.NoError(t, err)
		require.NoError(t, w.WriteHeader(&Header{
			Profile: "ros1",
			Library: libraryString,
		}))
		require.NoError(t, w.WriteSchema(&Schema{
			ID:       1,
			Name:     "foo",
			Encoding: "ros1msg",
			Data:     []byte{},
		}))
		for i := 0; i < 3; i++ {
			require.NoError(t, w.WriteChannel(&Channel{
				ID:              uint16(i),
				Topic:           fmt.Sprintf("/test-%d", i),
				MessageEncoding: "ros1",
				SchemaID:        1,
				Metadata:        map[string]string{},
			}))
		}
		for i := 0; i < 1000; i++ {
			channelID := uint16(i % 3)
			require.NoError(t, w.WriteMessage(&Message{
				ChannelID:   channelID,
				Sequence:    0,
				LogTime:     100,
				PublishTime: 100,
				Data: []byte{
					1,
					2,
					3,
					4,
				},
			}))
		}
		require.NoError(t, w.WriteAttachment(&Attachment{
			Name:      "file.jpg",
			LogTime:   0,
			MediaType: "image/jpeg",
			DataSize:  4,
			Data:      bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}),
		}))
		require.NoError(t, w.WriteAttachment(&Attachment{
			Name:      "file2.jpg",
			LogTime:   0,
			MediaType: "image/jpeg",
			DataSize:  4,
			Data:      bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}),
		}))
		require.NoError(t, w.Close())
		if i == 0 {
			hash = fmt.Sprintf("%x", md5.Sum(buf.Bytes()))
		}
		t.Run("output hashes consistently", func(t *testing.T) {
			newHash := fmt.Sprintf("%x", md5.Sum(buf.Bytes()))
			assert.Equal(t, hash, newHash)
		})
	}
}

func TestChunkedReadWrite(t *testing.T) {
	for _, compression := range []CompressionFormat{
		CompressionZSTD,
		CompressionLZ4,
		CompressionNone,
	} {
		t.Run(fmt.Sprintf("chunked file with %s", compression), func(t *testing.T) {
			buf := &bytes.Buffer{}
			w, err := NewWriter(buf, &WriterOptions{
				Chunked:         true,
				ChunkSize:       1,
				Compression:     compression,
				IncludeCRC:      true,
				OverrideLibrary: true,
			})
			require.NoError(t, err)
			require.NoError(t, w.WriteHeader(&Header{
				Profile: "ros1",
				Library: libraryString,
			}))
			require.NoError(t, w.WriteSchema(&Schema{
				ID:       1,
				Name:     "schema",
				Encoding: "msg",
				Data:     []byte{},
			}))
			require.NoError(t, w.WriteChannel(&Channel{
				ID:              1,
				Topic:           "/test",
				MessageEncoding: "ros1",
				SchemaID:        1,
				Metadata: map[string]string{
					"callerid": "100", // cspell:disable-line
				},
			}))
			require.NoError(t, w.WriteChannel(&Channel{
				ID:              2,
				Topic:           "/test2",
				MessageEncoding: "ros1",
				SchemaID:        1,
			}))
			require.NoError(t, w.WriteMessage(&Message{
				ChannelID:   1,
				Sequence:    0,
				LogTime:     100,
				PublishTime: 100,
				Data: []byte{
					1,
					2,
					3,
					4,
				},
			}))
			require.NoError(t, w.WriteMessage(&Message{
				ChannelID:   2,
				Sequence:    0,
				LogTime:     100,
				PublishTime: 100,
				Data: []byte{
					1,
					2,
					3,
					4,
				},
			}))
			require.NoError(t, w.Close())
			assert.Len(t, w.ChunkIndexes, 2)
			require.Empty(t, w.AttachmentIndexes)
			assert.Equal(t, uint64(2), w.Statistics.MessageCount)
			assert.Equal(t, uint32(0), w.Statistics.AttachmentCount)
			assert.Equal(t, uint32(2), w.Statistics.ChannelCount)
			assert.Equal(t, uint32(2), w.Statistics.ChunkCount)
			assert.Equal(t, int(w.Offset()), buf.Len())
			lexer, err := NewLexer(buf)
			require.NoError(t, err)
			defer lexer.Close()
			for i, expected := range []TokenType{
				TokenHeader,
				TokenSchema,
				TokenChannel,
				TokenChannel,
				TokenMessage,
				// Note: one message index per chunk, meaning that message indices for channels
				// not present in the chunk are not written.
				TokenMessageIndex,
				TokenMessage,
				TokenMessageIndex,
				TokenDataEnd,
				TokenSchema,
				TokenChannel,
				TokenChannel,
				TokenStatistics,
				TokenChunkIndex,
				TokenChunkIndex,
				TokenSummaryOffset,
				TokenSummaryOffset,
				TokenSummaryOffset,
				TokenSummaryOffset,
				TokenFooter,
			} {
				tokenType, _, err := lexer.Next(nil)
				require.NoError(t, err)
				assert.Equal(t, expected, tokenType,
					fmt.Sprintf("want %s got %s at %d", expected, tokenType, i))
			}
		})
	}
}

func TestChunkBoundaryIndexing(t *testing.T) {
	buf := &bytes.Buffer{}
	// Set a small chunk size so that every message will land in its own chunk.
	// Each chunk in the index should reflect the time of the corresponding
	// message.
	w, err := NewWriter(buf, &WriterOptions{
		Chunked:         true,
		ChunkSize:       20,
		Compression:     CompressionZSTD,
		OverrideLibrary: true,
	})
	require.NoError(t, err)
	err = w.WriteHeader(&Header{
		Profile: "ros1",
		Library: libraryString,
	})
	require.NoError(t, err)
	require.NoError(t, w.WriteSchema(&Schema{
		ID:       1,
		Name:     "schema",
		Data:     []byte{},
		Encoding: "msg",
	}))
	err = w.WriteChannel(&Channel{
		ID:              1,
		SchemaID:        1,
		Topic:           "/test",
		MessageEncoding: "ros1",
		Metadata:        make(map[string]string),
	})
	require.NoError(t, err)
	require.NoError(t, w.WriteMessage(&Message{
		ChannelID:   1,
		Sequence:    uint32(1),
		LogTime:     uint64(100),
		PublishTime: uint64(2),
		Data:        []byte("Hello, world!"),
	}))
	require.NoError(t, w.WriteMessage(&Message{
		ChannelID:   1,
		Sequence:    uint32(1),
		LogTime:     uint64(1),
		PublishTime: uint64(2),
		Data:        []byte("Hello, world!"),
	}))
	require.NoError(t, w.Close())
	t.Run("chunk indexes correct", func(t *testing.T) {
		assert.Len(t, w.ChunkIndexes, 2)
		assert.Equal(t, 100, int(w.ChunkIndexes[0].MessageStartTime)) // first message
		assert.Equal(t, 1, int(w.ChunkIndexes[1].MessageStartTime))   // second message
	})
}

func TestIndexStructures(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewWriter(buf, &WriterOptions{
		Chunked:         true,
		ChunkSize:       1024,
		Compression:     CompressionZSTD,
		OverrideLibrary: true,
	})
	require.NoError(t, err)
	err = w.WriteHeader(&Header{
		Profile: "ros1",
		Library: libraryString,
	})
	require.NoError(t, err)
	require.NoError(t, w.WriteSchema(&Schema{
		ID:       1,
		Name:     "schema",
		Data:     []byte{},
		Encoding: "msg",
	}))
	err = w.WriteChannel(&Channel{
		ID:              1,
		SchemaID:        1,
		Topic:           "/test",
		MessageEncoding: "ros1",
		Metadata:        make(map[string]string),
	})
	require.NoError(t, err)
	require.NoError(t, w.WriteMessage(&Message{
		ChannelID:   1,
		Sequence:    uint32(1),
		LogTime:     uint64(1),
		PublishTime: uint64(2),
		Data:        []byte("Hello, world!"),
	}))
	require.NoError(t, w.WriteAttachment(&Attachment{
		Name:       "file.jpg",
		LogTime:    100,
		CreateTime: 99,
		MediaType:  "image/jpeg",
		DataSize:   4,
		Data:       bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}),
	}))
	require.NoError(t, w.Close())
	t.Run("chunk indexes correct", func(t *testing.T) {
		assert.Len(t, w.ChunkIndexes, 1)
		chunkIndex := w.ChunkIndexes[0]
		assert.Equal(t, &ChunkIndex{
			MessageStartTime: 1,
			MessageEndTime:   1,
			ChunkStartOffset: 105,
			ChunkLength:      144,
			MessageIndexOffsets: map[uint16]uint64{
				1: 249,
			},
			MessageIndexLength: 31,
			Compression:        "zstd",
			CompressedSize:     91,
			UncompressedSize:   110,
		}, chunkIndex)
	})
	t.Run("attachment indexes correct", func(t *testing.T) {
		assert.Len(t, w.AttachmentIndexes, 1)
		attachmentIndex := w.AttachmentIndexes[0]
		assert.Equal(t, &AttachmentIndex{
			Offset:     38,
			Length:     67,
			LogTime:    100,
			CreateTime: 99,
			DataSize:   4,
			Name:       "file.jpg",
			MediaType:  "image/jpeg",
		}, attachmentIndex)
	})
}

func TestStatistics(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewWriter(buf, &WriterOptions{
		Chunked:         true,
		ChunkSize:       1024,
		Compression:     CompressionZSTD,
		OverrideLibrary: true,
	})
	require.NoError(t, err)
	require.NoError(t, w.WriteHeader(&Header{
		Profile: "ros1",
		Library: libraryString,
	}))
	require.NoError(t, w.WriteSchema(&Schema{
		ID:       1,
		Name:     "schema",
		Encoding: "msg",
		Data:     []byte{},
	}))
	require.NoError(t, w.WriteChannel(&Channel{
		ID:              1,
		SchemaID:        1,
		Topic:           "/test",
		MessageEncoding: "ros1",
		Metadata:        make(map[string]string),
	}))
	for i := 0; i < 1000; i++ {
		require.NoError(t, w.WriteMessage(&Message{
			ChannelID:   1,
			Sequence:    uint32(i),
			LogTime:     uint64(i),
			PublishTime: uint64(i),
			Data:        []byte("Hello, world!"),
		}))
	}
	require.NoError(t, w.WriteAttachment(&Attachment{
		Name:      "file.jpg",
		LogTime:   0,
		MediaType: "image/jpeg",
		DataSize:  4,
		Data:      bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}),
	}))

	// Write a message count for a channel that doesn't exist.
	w.Statistics.ChannelMessageCounts[100] = 1000

	require.NoError(t, w.Close())

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	info, err := reader.Info()
	require.NoError(t, err)

	assert.Equal(t, uint64(1000), info.Statistics.MessageCount)
	assert.Equal(t, uint32(1), info.Statistics.ChannelCount)
	assert.Equal(t, uint32(1), info.Statistics.AttachmentCount)
	assert.Equal(t, int(1), len(info.Statistics.ChannelMessageCounts))
	assert.Equal(t, uint64(1000), info.Statistics.ChannelMessageCounts[1])
	assert.Equal(t, 42, int(info.Statistics.ChunkCount))
	assert.Len(t, info.ChunkIndexes, 42)
	assert.Len(t, info.AttachmentIndexes, 1)
}

func TestUnchunkedReadWrite(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewWriter(buf, &WriterOptions{OverrideLibrary: true})
	require.NoError(t, err)
	err = w.WriteHeader(&Header{
		Profile: "ros1",
		Library: libraryString,
	})
	require.NoError(t, err)
	err = w.WriteSchema(&Schema{
		ID:       1,
		Name:     "schema",
		Encoding: "msg",
		Data:     []byte{},
	})
	require.NoError(t, err)
	err = w.WriteChannel(&Channel{
		ID:              1,
		SchemaID:        1,
		Topic:           "/test",
		MessageEncoding: "ros1",
		Metadata: map[string]string{
			"callerid": "100", // cspell:disable-line
		},
	})
	require.NoError(t, err)
	err = w.WriteMessage(&Message{
		ChannelID:   1,
		Sequence:    0,
		LogTime:     100,
		PublishTime: 100,
		Data: []byte{
			1,
			2,
			3,
			4,
		},
	})
	require.NoError(t, err)

	err = w.WriteAttachment(&Attachment{
		Name:      "file.jpg",
		LogTime:   0,
		MediaType: "image/jpeg",
		DataSize:  4,
		Data:      bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}),
	})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	require.Empty(t, w.ChunkIndexes)
	assert.Len(t, w.AttachmentIndexes, 1)
	assert.Equal(t, "image/jpeg", w.AttachmentIndexes[0].MediaType)
	assert.Equal(t, uint64(1), w.Statistics.MessageCount)
	assert.Equal(t, uint32(1), w.Statistics.AttachmentCount)
	assert.Equal(t, uint32(1), w.Statistics.ChannelCount)
	assert.Equal(t, uint32(0), w.Statistics.ChunkCount)

	lexer, err := NewLexer(buf)
	require.NoError(t, err)
	defer lexer.Close()
	for _, expected := range []TokenType{
		TokenHeader,
		TokenSchema,
		TokenChannel,
		TokenMessage,
		TokenDataEnd,
		TokenSchema,
		TokenChannel,
		TokenStatistics,
		TokenAttachmentIndex,
		TokenSummaryOffset,
		TokenSummaryOffset,
		TokenSummaryOffset,
		TokenSummaryOffset,
		TokenFooter,
	} {
		tokenType, _, err := lexer.Next(nil)
		require.NoError(t, err)
		assert.Equal(t, expected, tokenType, fmt.Sprintf("want %s got %s", expected, tokenType))
	}
}

func TestLibraryString(t *testing.T) {
	thisLibraryString := fmt.Sprintf("mcap go %s", Version)
	cases := []struct {
		input  string
		output string
	}{
		{"", thisLibraryString},
		{thisLibraryString, thisLibraryString},
		{"some-library", fmt.Sprintf("%s; some-library", thisLibraryString)},
	}
	for _, c := range cases {
		t.Run("library string is automatically filled", func(t *testing.T) {
			buf := &bytes.Buffer{}
			w, err := NewWriter(buf, &WriterOptions{})
			require.NoError(t, err)
			err = w.WriteHeader(&Header{
				Profile: "ros1",
				Library: c.input,
			})
			require.NoError(t, err)
			w.Close()
			lexer, err := NewLexer(buf)
			require.NoError(t, err)
			defer lexer.Close()
			tokenType, record, err := lexer.Next(nil)
			require.NoError(t, err)
			assert.Equal(t, TokenHeader, tokenType)
			offset := 0
			profile, offset, err := getPrefixedString(record, offset)
			require.NoError(t, err)
			assert.Equal(t, "ros1", profile)
			library, _, err := getPrefixedString(record, offset)
			require.NoError(t, err)
			assert.Equal(t, library, c.output)
		})
	}
}

func TestMakePrefixedMap(t *testing.T) {
	t.Run("output is deterministic", func(t *testing.T) {
		bytes := makePrefixedMap(map[string]string{
			"foo": "bar",
			"bar": "foo",
		})
		assert.Equal(t, flatten(
			encodedUint32(2*4+2*4+4*3), // map length
			encodedUint32(3),
			[]byte("bar"),
			encodedUint32(3),
			[]byte("foo"),
			encodedUint32(3),
			[]byte("foo"),
			encodedUint32(3),
			[]byte("bar"),
		), bytes)
	})
}

func BenchmarkWriterAllocs(b *testing.B) {
	cases := []struct {
		assertion    string
		chunkSize    int
		messageCount int
		channelCount int
	}{
		{
			"big chunks many messages",
			8 * 1024 * 1024,
			2e6,
			100,
		},
		{
			"small chunks many messages",
			8 * 1024,
			2e6,
			100,
		},
		{
			"many channels",
			4 * 1024 * 1024,
			2e6,
			55000,
		},
	}

	stringData := "hello, world!"
	messageData := []byte("hello, world")
	buf := bytes.NewBuffer(make([]byte, 4*1024*1024*1024))
	for _, c := range cases {
		b.ResetTimer()
		b.Run(c.assertion, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				t0 := time.Now()
				writer, err := NewWriter(buf, &WriterOptions{
					ChunkSize: int64(c.chunkSize),
					Chunked:   true,
				})
				require.NoError(b, err)
				require.NoError(b, writer.WriteHeader(&Header{
					Profile: "ros1",
					Library: "foo",
				}))
				for i := 0; i < c.channelCount; i++ {
					require.NoError(b, writer.WriteSchema(&Schema{
						ID:       uint16(i),
						Name:     stringData,
						Encoding: "ros1msg",
						Data:     messageData,
					}))
					require.NoError(b, writer.WriteChannel(&Channel{
						ID:              uint16(i),
						SchemaID:        uint16(i),
						Topic:           stringData,
						MessageEncoding: "msg",
						Metadata: map[string]string{
							"": "",
						},
					}))
				}
				channelID := 0
				messageCount := 0
				for messageCount < c.messageCount {
					require.NoError(b, writer.WriteMessage(&Message{
						ChannelID:   uint16(channelID),
						Sequence:    0,
						LogTime:     uint64(messageCount),
						PublishTime: uint64(messageCount),
						Data:        messageData,
					}))
					messageCount++
					channelID++
					channelID %= c.channelCount
				}
				writer.Close()
				elapsed := time.Since(t0)
				b.ReportMetric(float64(c.messageCount)/elapsed.Seconds(), "messages/sec")
			}
		})
	}
}

func TestWriteAttachment(t *testing.T) {
	cases := []struct {
		assertion  string
		attachment *Attachment
		err        error
	}{
		{
			"incorrect content size",
			&Attachment{
				DataSize: 2,
				Data:     bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}),
			},
			ErrAttachmentDataSizeIncorrect,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf := &bytes.Buffer{}
			writer, err := NewWriter(buf, &WriterOptions{})
			require.NoError(t, err)
			err = writer.WriteAttachment(c.attachment)
			require.ErrorIs(t, err, c.err)
		})
	}
}

func assertReadable(t *testing.T, rs io.ReadSeeker) {
	reader, err := NewReader(rs)
	require.NoError(t, err)

	_, err = reader.Info()
	require.NoError(t, err)

	it, err := reader.Messages()
	require.NoError(t, err)
	for {
		_, _, _, err := it.Next(nil)
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}
	}
}

func TestBYOCompressor(t *testing.T) {
	buf := &bytes.Buffer{}
	// example - custom lz4 settings
	lzw := lz4.NewWriter(nil)
	blockCount := 0
	require.NoError(t, lzw.Apply(lz4.OnBlockDoneOption(func(int) {
		blockCount++
	})))

	writer, err := NewWriter(buf, &WriterOptions{
		Chunked:    true,
		ChunkSize:  1024,
		Compressor: NewCustomCompressor("lz4", lzw),
	})
	require.NoError(t, err)

	require.NoError(t, writer.WriteHeader(&Header{}))
	require.NoError(t, writer.WriteSchema(&Schema{
		ID:       1,
		Name:     "schema",
		Encoding: "ros1msg",
		Data:     []byte{},
	}))
	require.NoError(t, writer.WriteChannel(&Channel{
		ID:              0,
		SchemaID:        1,
		Topic:           "/foo",
		MessageEncoding: "ros1msg",
	}))

	for i := 0; i < 100; i++ {
		require.NoError(t, writer.WriteMessage(&Message{
			ChannelID: 0,
			Sequence:  0,
			LogTime:   uint64(i),
		}))
	}
	require.NoError(t, writer.Close())
	assertReadable(t, bytes.NewReader(buf.Bytes()))
	assert.Positive(t, blockCount)
}

func BenchmarkManyWriterAllocs(b *testing.B) {
	cases := []struct {
		assertion    string
		chunkSize    int
		messageCount int
		channelCount int
	}{
		{
			"big chunks many messages",
			8 * 1024 * 1024,
			2e6,
			100,
		},
		{
			"small chunks many messages",
			8 * 1024,
			2e6,
			100,
		},
		{
			"many channels",
			4 * 1024 * 1024,
			2e6,
			55000,
		},
	}

	stringData := "hello, world!"
	messageData := []byte("hello, world")
	schema := Schema{
		Name:     stringData,
		Encoding: "ros1msg",
		Data:     messageData,
	}
	channel := Channel{
		Topic:           stringData,
		MessageEncoding: "msg",
		Metadata: map[string]string{
			"": "",
		},
	}
	message := Message{
		Sequence: 0,
		Data:     messageData,
	}
	writers := make([]*Writer, 100)
	for _, c := range cases {
		b.ResetTimer()
		b.Run(c.assertion, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				t0 := time.Now()
				for i := 0; i < len(writers); i++ {
					writer, err := NewWriter(io.Discard, &WriterOptions{
						ChunkSize: int64(c.chunkSize),
						Chunked:   true,
					})
					require.NoError(b, err)
					require.NoError(b, writer.WriteHeader(&Header{
						Profile: "ros1",
						Library: "foo",
					}))
					for j := 0; j < c.channelCount; j++ {
						schema.ID = uint16(j + 1)
						require.NoError(b, writer.WriteSchema(&schema))
						channel.SchemaID = uint16(j + 1)
						channel.ID = uint16(j)
						require.NoError(b, writer.WriteChannel(&channel))
					}
					writers[i] = writer
				}
				channelID := 0
				messageCount := 0
				for messageCount < c.messageCount {
					writerIdx := messageCount % len(writers)
					message.ChannelID = uint16(channelID)
					message.LogTime = uint64(messageCount)
					message.PublishTime = uint64(messageCount)
					require.NoError(b, writers[writerIdx].WriteMessage(&message))
					messageCount++
					channelID++
					channelID %= c.channelCount
				}
				for _, writer := range writers {
					require.NoError(b, writer.Close())
				}
				elapsed := time.Since(t0)
				b.ReportMetric(float64(c.messageCount)/elapsed.Seconds(), "messages/sec")
			}
		})
	}
}
