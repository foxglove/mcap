package libmcap

import (
	"bytes"
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
					"callerid": "100",
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
			assert.Nil(t, w.Close())
			assert.Nil(t, err)
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
			"callerid": "100",
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
