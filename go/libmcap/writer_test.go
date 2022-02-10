package libmcap

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMCAPReadWrite(t *testing.T) {
	t.Run("test header", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w, err := NewWriter(buf, &WriterOptions{Compression: CompressionZSTD})
		assert.Nil(t, err)
		err = w.WriteHeader(&Header{
			Profile: "ros1",
		})
		assert.Nil(t, err)
		lexer, err := NewLexer(buf)
		assert.Nil(t, err)
		tokenType, record, err := lexer.Next(nil)
		assert.Nil(t, err)
		// body of the header is the profile, followed by the metadata map
		offset := 0
		profile, offset, err := readPrefixedString(record, offset)
		assert.Nil(t, err)
		assert.Equal(t, "ros1", profile)
		library, _, err := readPrefixedString(record, offset)
		assert.Nil(t, err)
		assert.Equal(t, "", library)
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
			Chunked:     true,
			Compression: CompressionZSTD,
			IncludeCRC:  true,
			ChunkSize:   1024,
		})
		assert.Nil(t, err)
		assert.Nil(t, w.WriteHeader(&Header{
			Profile: "ros1",
		}))
		assert.Nil(t, w.WriteSchema(&Schema{
			ID:       1,
			Name:     "foo",
			Encoding: "ros1",
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
			Name:        "file.jpg",
			LogTime:     0,
			ContentType: "image/jpeg",
			Data:        []byte{0x01, 0x02, 0x03, 0x04},
		}))
		assert.Nil(t, w.WriteAttachment(&Attachment{
			Name:        "file2.jpg",
			LogTime:     0,
			ContentType: "image/jpeg",
			Data:        []byte{0x01, 0x02, 0x03, 0x04},
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
				Chunked:     true,
				Compression: compression,
				IncludeCRC:  true,
			})
			assert.Nil(t, err)
			assert.Nil(t, w.WriteHeader(&Header{
				Profile: "ros1",
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
			lexer, err := NewLexer(buf)
			assert.Nil(t, err)
			for i, expected := range []TokenType{
				TokenHeader,
				TokenSchema,
				TokenChannel,
				TokenMessage,
				TokenMessageIndex,
				TokenDataEnd,
				TokenChannel,
				TokenSchema,
				TokenChunkIndex,
				TokenStatistics,
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

func TestIndexStructures(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewWriter(buf, &WriterOptions{
		Chunked:     true,
		ChunkSize:   1024,
		Compression: CompressionZSTD,
	})
	assert.Nil(t, err)
	err = w.WriteHeader(&Header{
		Profile: "ros1",
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
		Name:        "file.jpg",
		LogTime:     100,
		ContentType: "image/jpeg",
		Data:        []byte{0x01, 0x02, 0x03, 0x04},
	}))
	assert.Nil(t, w.Close())
	t.Run("chunk indexes correct", func(t *testing.T) {
		assert.Equal(t, 1, len(w.ChunkIndexes))
		chunkIndex := w.ChunkIndexes[0]
		assert.Equal(t, &ChunkIndex{
			MessageStartTime: 1,
			MessageEndTime:   1,
			ChunkStartOffset: 96,
			ChunkLength:      144,
			MessageIndexOffsets: map[uint16]uint64{
				1: 240,
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
			Offset:      29,
			Length:      67,
			LogTime:     100,
			DataSize:    4,
			Name:        "file.jpg",
			ContentType: "image/jpeg",
		}, attachmentIndex)
	})
}

func TestStatistics(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewWriter(buf, &WriterOptions{
		Chunked:     true,
		ChunkSize:   1024,
		Compression: CompressionZSTD,
	})
	assert.Nil(t, err)
	assert.Nil(t, w.WriteHeader(&Header{
		Profile: "ros1",
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
		Name:        "file.jpg",
		LogTime:     0,
		ContentType: "image/jpeg",
		Data:        []byte{0x01, 0x02, 0x03, 0x04},
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
	w, err := NewWriter(buf, &WriterOptions{})
	assert.Nil(t, err)
	err = w.WriteHeader(&Header{
		Profile: "ros1",
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
		Name:        "file.jpg",
		LogTime:     0,
		ContentType: "image/jpeg",
		Data:        []byte{0x01, 0x02, 0x03, 0x04},
	})
	assert.Nil(t, err)
	assert.Nil(t, w.Close())

	assert.Equal(t, 0, len(w.ChunkIndexes))
	assert.Equal(t, 1, len(w.AttachmentIndexes))
	assert.Equal(t, uint64(1), w.Statistics.MessageCount)
	assert.Equal(t, uint32(1), w.Statistics.AttachmentCount)
	assert.Equal(t, uint32(1), w.Statistics.ChannelCount)
	assert.Equal(t, uint32(0), w.Statistics.ChunkCount)

	lexer, err := NewLexer(buf)
	assert.Nil(t, err)
	for _, expected := range []TokenType{
		TokenHeader,
		TokenSchema,
		TokenChannel,
		TokenMessage,
		TokenAttachment,
		TokenDataEnd,
		TokenChannel,
		TokenSchema,
		TokenAttachmentIndex,
		TokenStatistics,
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
