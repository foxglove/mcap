package mcap

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const libraryString = "libfoo v0"

func TestMCAPReadWrite(t *testing.T) {
	t.Run("test header", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w, err := NewWriter(buf, &WriterOptions{Compression: CompressionZSTD, OverrideLibrary: true})
		assert.Nil(t, err)
		err = w.WriteHeader(&Header{
			Profile: "ros1",
			Library: libraryString,
		})
		assert.Nil(t, err)
		lexer, err := NewLexer(buf)
		assert.Nil(t, err)
		defer lexer.Close()
		tokenType, record, err := lexer.Next(nil)
		assert.Nil(t, err)
		// body of the header is the profile, followed by the metadata map
		offset := 0
		profile, offset, err := getPrefixedString(record, offset)
		assert.Nil(t, err)
		assert.Equal(t, "ros1", profile)
		library, _, err := getPrefixedString(record, offset)
		assert.Nil(t, err)
		assert.Equal(t, "libfoo v0", library)
		assert.Equal(t, TokenHeader, tokenType)
	})
	t.Run("zero-valued schema IDs permitted", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w, err := NewWriter(buf, &WriterOptions{Compression: CompressionLZ4})
		assert.Nil(t, err)
		err = w.WriteChannel(&Channel{
			ID:              0,
			SchemaID:        0,
			Topic:           "/foo",
			MessageEncoding: "msg",
			Metadata: map[string]string{
				"key": "val",
			},
		})
		assert.Nil(t, err)
	})
	t.Run("positive schema IDs rejected if schema unknown", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w, err := NewWriter(buf, &WriterOptions{Compression: CompressionLZ4})
		assert.Nil(t, err)
		err = w.WriteChannel(&Channel{
			ID:              0,
			SchemaID:        1,
			Topic:           "/foo",
			MessageEncoding: "msg",
			Metadata: map[string]string{
				"key": "val",
			},
		})
		assert.ErrorIs(t, err, ErrUnknownSchema)
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
		assert.Nil(t, err)
		assert.Nil(t, w.WriteHeader(&Header{
			Profile: "ros1",
			Library: libraryString,
		}))
		assert.Nil(t, w.WriteSchema(&Schema{
			ID:       1,
			Name:     "foo",
			Encoding: "ros1msg",
			Data:     []byte{},
		}))
		for i := 0; i < 3; i++ {
			assert.Nil(t, w.WriteChannel(&Channel{
				ID:              uint16(i),
				Topic:           fmt.Sprintf("/test-%d", i),
				MessageEncoding: "ros1",
				SchemaID:        1,
				Metadata:        map[string]string{},
			}))
		}
		for i := 0; i < 1000; i++ {
			channelID := uint16(i % 3)
			assert.Nil(t, w.WriteMessage(&Message{
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
		assert.Nil(t, w.WriteAttachment(&Attachment{
			Name:      "file.jpg",
			LogTime:   0,
			MediaType: "image/jpeg",
			DataSize:  4,
			Data:      bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}),
		}))
		assert.Nil(t, w.WriteAttachment(&Attachment{
			Name:      "file2.jpg",
			LogTime:   0,
			MediaType: "image/jpeg",
			DataSize:  4,
			Data:      bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}),
		}))
		assert.Nil(t, w.Close())
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
				Compression:     compression,
				IncludeCRC:      true,
				OverrideLibrary: true,
			})
			assert.Nil(t, err)
			assert.Nil(t, w.WriteHeader(&Header{
				Profile: "ros1",
				Library: libraryString,
			}))
			assert.Nil(t, w.WriteSchema(&Schema{
				ID:       1,
				Name:     "schema",
				Encoding: "msg",
				Data:     []byte{},
			}))
			assert.Nil(t, w.WriteChannel(&Channel{
				ID:              1,
				Topic:           "/test",
				MessageEncoding: "ros1",
				SchemaID:        1,
				Metadata: map[string]string{
					"callerid": "100", // cspell:disable-line
				},
			}))
			assert.Nil(t, w.WriteMessage(&Message{
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
			assert.Nil(t, w.Close())
			assert.Equal(t, 1, len(w.ChunkIndexes))
			assert.Equal(t, 0, len(w.AttachmentIndexes))
			assert.Equal(t, uint64(1), w.Statistics.MessageCount)
			assert.Equal(t, uint32(0), w.Statistics.AttachmentCount)
			assert.Equal(t, uint32(1), w.Statistics.ChannelCount)
			assert.Equal(t, uint32(1), w.Statistics.ChunkCount)
			assert.Equal(t, int(w.Offset()), buf.Len())
			lexer, err := NewLexer(buf)
			assert.Nil(t, err)
			defer lexer.Close()
			for i, expected := range []TokenType{
				TokenHeader,
				TokenSchema,
				TokenChannel,
				TokenMessage,
				TokenMessageIndex,
				TokenDataEnd,
				TokenSchema,
				TokenChannel,
				TokenStatistics,
				TokenChunkIndex,
				TokenSummaryOffset,
				TokenSummaryOffset,
				TokenSummaryOffset,
				TokenSummaryOffset,
				TokenFooter,
			} {
				tokenType, _, err := lexer.Next(nil)
				assert.Nil(t, err)
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
	assert.Nil(t, err)
	err = w.WriteHeader(&Header{
		Profile: "ros1",
		Library: libraryString,
	})
	assert.Nil(t, err)
	assert.Nil(t, w.WriteSchema(&Schema{
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
	assert.Nil(t, err)
	assert.Nil(t, w.WriteMessage(&Message{
		ChannelID:   1,
		Sequence:    uint32(1),
		LogTime:     uint64(100),
		PublishTime: uint64(2),
		Data:        []byte("Hello, world!"),
	}))
	assert.Nil(t, w.WriteMessage(&Message{
		ChannelID:   1,
		Sequence:    uint32(1),
		LogTime:     uint64(1),
		PublishTime: uint64(2),
		Data:        []byte("Hello, world!"),
	}))
	assert.Nil(t, w.Close())
	t.Run("chunk indexes correct", func(t *testing.T) {
		assert.Equal(t, 2, len(w.ChunkIndexes))
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
	assert.Nil(t, err)
	err = w.WriteHeader(&Header{
		Profile: "ros1",
		Library: libraryString,
	})
	assert.Nil(t, err)
	assert.Nil(t, w.WriteSchema(&Schema{
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
	assert.Nil(t, err)
	assert.Nil(t, w.WriteMessage(&Message{
		ChannelID:   1,
		Sequence:    uint32(1),
		LogTime:     uint64(1),
		PublishTime: uint64(2),
		Data:        []byte("Hello, world!"),
	}))
	assert.Nil(t, w.WriteAttachment(&Attachment{
		Name:       "file.jpg",
		LogTime:    100,
		CreateTime: 99,
		MediaType:  "image/jpeg",
		DataSize:   4,
		Data:       bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}),
	}))
	assert.Nil(t, w.Close())
	t.Run("chunk indexes correct", func(t *testing.T) {
		assert.Equal(t, 1, len(w.ChunkIndexes))
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
		assert.Equal(t, 1, len(w.AttachmentIndexes))
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
	assert.Nil(t, err)
	assert.Nil(t, w.WriteHeader(&Header{
		Profile: "ros1",
		Library: libraryString,
	}))
	assert.Nil(t, w.WriteSchema(&Schema{
		ID:       1,
		Name:     "schema",
		Encoding: "msg",
		Data:     []byte{},
	}))
	assert.Nil(t, w.WriteChannel(&Channel{
		ID:              1,
		SchemaID:        1,
		Topic:           "/test",
		MessageEncoding: "ros1",
		Metadata:        make(map[string]string),
	}))
	for i := 0; i < 1000; i++ {
		assert.Nil(t, w.WriteMessage(&Message{
			ChannelID:   1,
			Sequence:    uint32(i),
			LogTime:     uint64(i),
			PublishTime: uint64(i),
			Data:        []byte("Hello, world!"),
		}))
	}
	assert.Nil(t, w.WriteAttachment(&Attachment{
		Name:      "file.jpg",
		LogTime:   0,
		MediaType: "image/jpeg",
		DataSize:  4,
		Data:      bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}),
	}))
	assert.Nil(t, w.Close())
	assert.Equal(t, uint64(1000), w.Statistics.MessageCount)
	assert.Equal(t, uint32(1), w.Statistics.ChannelCount)
	assert.Equal(t, uint32(1), w.Statistics.AttachmentCount)
	assert.Equal(t, 42, int(w.Statistics.ChunkCount))
	assert.Equal(t, 42, len(w.ChunkIndexes))
	assert.Equal(t, 1, len(w.AttachmentIndexes))
}

func TestUnchunkedReadWrite(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewWriter(buf, &WriterOptions{OverrideLibrary: true})
	assert.Nil(t, err)
	err = w.WriteHeader(&Header{
		Profile: "ros1",
		Library: libraryString,
	})
	assert.Nil(t, err)
	err = w.WriteSchema(&Schema{
		ID:       1,
		Name:     "schema",
		Encoding: "msg",
		Data:     []byte{},
	})
	assert.Nil(t, err)
	err = w.WriteChannel(&Channel{
		ID:              1,
		SchemaID:        1,
		Topic:           "/test",
		MessageEncoding: "ros1",
		Metadata: map[string]string{
			"callerid": "100", // cspell:disable-line
		},
	})
	assert.Nil(t, err)
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
	assert.Nil(t, err)

	err = w.WriteAttachment(&Attachment{
		Name:      "file.jpg",
		LogTime:   0,
		MediaType: "image/jpeg",
		DataSize:  4,
		Data:      bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}),
	})
	assert.Nil(t, err)
	assert.Nil(t, w.Close())

	assert.Equal(t, 0, len(w.ChunkIndexes))
	assert.Equal(t, 1, len(w.AttachmentIndexes))
	assert.Equal(t, "image/jpeg", w.AttachmentIndexes[0].MediaType)
	assert.Equal(t, uint64(1), w.Statistics.MessageCount)
	assert.Equal(t, uint32(1), w.Statistics.AttachmentCount)
	assert.Equal(t, uint32(1), w.Statistics.ChannelCount)
	assert.Equal(t, uint32(0), w.Statistics.ChunkCount)

	lexer, err := NewLexer(buf)
	assert.Nil(t, err)
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
		assert.Nil(t, err)
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
			assert.Nil(t, err)
			err = w.WriteHeader(&Header{
				Profile: "ros1",
				Library: c.input,
			})
			assert.Nil(t, err)
			w.Close()
			lexer, err := NewLexer(buf)
			assert.Nil(t, err)
			defer lexer.Close()
			tokenType, record, err := lexer.Next(nil)
			assert.Nil(t, err)
			assert.Equal(t, tokenType, TokenHeader)
			offset := 0
			profile, offset, err := getPrefixedString(record, offset)
			assert.Nil(t, err)
			assert.Equal(t, "ros1", profile)
			library, _, err := getPrefixedString(record, offset)
			assert.Nil(t, err)
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
				assert.Nil(b, err)
				assert.Nil(b, writer.WriteHeader(&Header{
					Profile: "ros1",
					Library: "foo",
				}))
				for i := 0; i < c.channelCount; i++ {
					assert.Nil(b, writer.WriteSchema(&Schema{
						ID:       uint16(i),
						Name:     stringData,
						Encoding: "ros1msg",
						Data:     messageData,
					}))
					assert.Nil(b, writer.WriteChannel(&Channel{
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
					assert.Nil(b, writer.WriteMessage(&Message{
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
			assert.Nil(t, err)
			err = writer.WriteAttachment(c.attachment)
			assert.ErrorIs(t, err, c.err)
		})
	}
}
