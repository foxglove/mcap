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
		w, err := NewWriter(buf, &WriterOptions{Compression: CompressionLZ4})
		assert.Nil(t, err)
		err = w.WriteHeader(&Header{
			Profile: "ros1",
		})
		assert.Nil(t, err)
		lexer, err := NewLexer(buf)
		assert.Nil(t, err)
		token, err := lexer.Next()
		assert.Nil(t, err)
		// body of the header is the profile, followed by the metadata map
		offset := 0
		data := token.bytes()
		profile, offset, err := readPrefixedString(data, offset)
		assert.Nil(t, err)
		assert.Equal(t, "ros1", profile)
		library, _, err := readPrefixedString(data, offset)
		assert.Nil(t, err)
		assert.Equal(t, "", library)
		assert.Equal(t, TokenHeader, token.TokenType)
	})
}

func TestOutputDeterminism(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewWriter(buf, &WriterOptions{
		Chunked:     true,
		Compression: CompressionLZ4,
		IncludeCRC:  true,
		ChunkSize:   1024,
	})
	assert.Nil(t, err)
	assert.Nil(t, w.WriteHeader(&Header{
		Profile: "ros1",
	}))
	for i := 0; i < 3; i++ {
		assert.Nil(t, w.WriteChannelInfo(&ChannelInfo{
			ChannelID:       uint16(i),
			TopicName:       fmt.Sprintf("/test-%d", i),
			MessageEncoding: "ros1",
			SchemaName:      "foo",
			Schema:          []byte{},
			Metadata:        map[string]string{},
		}))
	}
	for i := 0; i < 1000; i++ {
		channelID := uint16(i % 3)
		assert.Nil(t, w.WriteMessage(&Message{
			ChannelID:   channelID,
			Sequence:    0,
			RecordTime:  100,
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
		RecordTime:  0,
		ContentType: "image/jpeg",
		Data:        []byte{0x01, 0x02, 0x03, 0x04},
	}))
	assert.Nil(t, w.WriteAttachment(&Attachment{
		Name:        "file2.jpg",
		RecordTime:  0,
		ContentType: "image/jpeg",
		Data:        []byte{0x01, 0x02, 0x03, 0x04},
	}))
	assert.Nil(t, w.Close())
	t.Run("output hashes consistently", func(t *testing.T) {
		hash := md5.Sum(buf.Bytes())
		assert.Equal(t, "e88a065b525c10d7d9d42f136c4475d1", fmt.Sprintf("%x", hash))
	})
}

func TestChunkedReadWrite(t *testing.T) {
	for _, compression := range []CompressionFormat{
		CompressionLZ4,
		CompressionZSTD,
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
			err = w.WriteHeader(&Header{
				Profile: "ros1",
			})
			assert.Nil(t, err)
			err = w.WriteChannelInfo(&ChannelInfo{
				ChannelID:       1,
				TopicName:       "/test",
				MessageEncoding: "ros1",
				SchemaName:      "foo",
				Schema:          []byte{},
				Metadata: map[string]string{
					"callerid": "100", // cspell:disable-line
				},
			})
			assert.Nil(t, err)
			err = w.WriteMessage(&Message{
				ChannelID:   1,
				Sequence:    0,
				RecordTime:  100,
				PublishTime: 100,
				Data: []byte{
					1,
					2,
					3,
					4,
				},
			})
			assert.Nil(t, err)
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
				TokenChannelInfo,
				TokenMessage,
				TokenChannelInfo,
				TokenStatistics,
				TokenSummaryOffset,
				TokenSummaryOffset,
				TokenSummaryOffset,
				TokenFooter,
			} {
				tok, err := lexer.Next()
				assert.Nil(t, err)
				_ = tok.bytes() // need to read the data
				assert.Equal(t, expected, tok.TokenType,
					fmt.Sprintf("want %s got %s at %d", Token{expected, 0, nil}, tok.TokenType, i))
			}
		})
	}
}

func TestIndexStructures(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewWriter(buf, &WriterOptions{
		Chunked:     true,
		ChunkSize:   1024,
		Compression: CompressionLZ4,
	})
	assert.Nil(t, err)
	err = w.WriteHeader(&Header{
		Profile: "ros1",
	})
	assert.Nil(t, err)
	err = w.WriteChannelInfo(&ChannelInfo{
		ChannelID:       1,
		TopicName:       "/test",
		MessageEncoding: "ros1",
		SchemaName:      "foo",
		Schema:          []byte{},
		Metadata:        make(map[string]string),
	})
	assert.Nil(t, err)
	assert.Nil(t, w.WriteMessage(&Message{
		ChannelID:   1,
		Sequence:    uint32(1),
		RecordTime:  uint64(1),
		PublishTime: uint64(2),
		Data:        []byte("Hello, world!"),
	}))
	assert.Nil(t, w.WriteAttachment(&Attachment{
		Name:        "file.jpg",
		RecordTime:  100,
		ContentType: "image/jpeg",
		Data:        []byte{0x01, 0x02, 0x03, 0x04},
	}))
	assert.Nil(t, w.Close())
	t.Run("chunk indexes correct", func(t *testing.T) {
		assert.Equal(t, 1, len(w.ChunkIndexes))
		chunkIndex := w.ChunkIndexes[0]
		assert.Equal(t, &ChunkIndex{
			StartTime:        1,
			EndTime:          1,
			ChunkStartOffset: 96,
			ChunkLength:      143,
			MessageIndexOffsets: map[uint16]uint64{
				1: 239,
			},
			MessageIndexLength: 31,
			Compression:        "lz4",
			CompressedSize:     99,
			UncompressedSize:   91,
		}, chunkIndex)
	})
	t.Run("attachment indexes correct", func(t *testing.T) {
		assert.Equal(t, 1, len(w.AttachmentIndexes))
		attachmentIndex := w.AttachmentIndexes[0]
		assert.Equal(t, &AttachmentIndex{
			Offset:      29,
			Length:      67,
			RecordTime:  100,
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
		Compression: CompressionLZ4,
	})
	assert.Nil(t, err)
	err = w.WriteHeader(&Header{
		Profile: "ros1",
	})
	assert.Nil(t, err)
	err = w.WriteChannelInfo(&ChannelInfo{
		ChannelID:       1,
		TopicName:       "/test",
		MessageEncoding: "ros1",
		SchemaName:      "foo",
		Schema:          []byte{},
		Metadata:        make(map[string]string),
	})
	assert.Nil(t, err)
	for i := 0; i < 1000; i++ {
		assert.Nil(t, w.WriteMessage(&Message{
			ChannelID:   1,
			Sequence:    uint32(i),
			RecordTime:  uint64(i),
			PublishTime: uint64(i),
			Data:        []byte("Hello, world!"),
		}))
	}
	assert.Nil(t, w.WriteAttachment(&Attachment{
		Name:        "file.jpg",
		RecordTime:  0,
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
	err = w.WriteChannelInfo(&ChannelInfo{
		ChannelID:       1,
		TopicName:       "/test",
		MessageEncoding: "ros1",
		SchemaName:      "foo",
		Schema:          []byte{},
		Metadata: map[string]string{
			"callerid": "100", // cspell:disable-line
		},
	})
	assert.Nil(t, err)
	err = w.WriteMessage(&Message{
		ChannelID:   1,
		Sequence:    0,
		RecordTime:  100,
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
		RecordTime:  0,
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
		TokenChannelInfo,
		TokenMessage,
		TokenAttachment,
		TokenChannelInfo,
		TokenAttachmentIndex,
		TokenStatistics,
		TokenSummaryOffset,
		TokenSummaryOffset,
		TokenSummaryOffset,
		TokenFooter,
	} {
		tok, err := lexer.Next()
		assert.Nil(t, err)
		_ = tok.bytes()
		assert.Equal(t, expected, tok.TokenType, fmt.Sprintf("want %s got %s", Token{expected, 0, nil}, tok))
	}
}
