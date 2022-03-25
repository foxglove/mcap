package mcap

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	kilobyte = 1024
	megabyte = 1024 * kilobyte
	gigabyte = 1024 * megabyte
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
		assert.Equal(t, "mcap go #", library)
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
			ChunkSize:   kilobyte,
		})
		assert.Nil(t, err)
		assert.Nil(t, w.WriteHeader(&Header{
			Profile: "ros1",
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

func TestSortChunk(t *testing.T) {
	dummyOpcode := []byte{0x00}
	cases := []struct {
		assertion   string
		chunk       []byte
		index       []messageIndexEntry
		outputChunk []byte
		outputIndex []messageIndexEntry
	}{
		{
			"same size messages",
			flatten(
				dummyOpcode, encodedUint64(4), []byte("2222"),
				dummyOpcode, encodedUint64(4), []byte("1111"),
				dummyOpcode, encodedUint64(4), []byte("3333"),
			),
			[]messageIndexEntry{
				newMessageIndexEntry(0, 2, 0),
				newMessageIndexEntry(13, 1, 0),
				newMessageIndexEntry(26, 3, 0),
			},
			flatten(
				dummyOpcode, encodedUint64(4), []byte("1111"),
				dummyOpcode, encodedUint64(4), []byte("2222"),
				dummyOpcode, encodedUint64(4), []byte("3333"),
			),
			[]messageIndexEntry{
				newMessageIndexEntry(0, 1, 0),
				newMessageIndexEntry(13, 2, 0),
				newMessageIndexEntry(26, 3, 0),
			},
		},
		{
			"left message shorter",
			flatten(
				dummyOpcode, encodedUint64(3), []byte("222"),
				dummyOpcode, encodedUint64(4), []byte("1111"),
				dummyOpcode, encodedUint64(4), []byte("3333"),
			),
			[]messageIndexEntry{
				newMessageIndexEntry(0, 2, 0),
				newMessageIndexEntry(12, 1, 0),
				newMessageIndexEntry(25, 3, 0),
			},
			flatten(
				dummyOpcode, encodedUint64(4), []byte("1111"),
				dummyOpcode, encodedUint64(3), []byte("222"),
				dummyOpcode, encodedUint64(4), []byte("3333"),
			),
			[]messageIndexEntry{
				newMessageIndexEntry(0, 1, 0),
				newMessageIndexEntry(13, 2, 0),
				newMessageIndexEntry(25, 3, 0),
			},
		},
		{
			"noncontiguous messages",
			flatten(
				dummyOpcode, encodedUint64(4), []byte("2222"),
				[]byte{0x00, 0x00, 0x00, 0x00},
				dummyOpcode, encodedUint64(3), []byte("111"),
				[]byte{0x00, 0x00, 0x00, 0x00},
				dummyOpcode, encodedUint64(4), []byte("3333"),
			),
			[]messageIndexEntry{
				newMessageIndexEntry(0, 2, 0),
				newMessageIndexEntry(17, 1, 0),
				newMessageIndexEntry(33, 3, 0),
			},
			flatten(
				dummyOpcode, encodedUint64(3), []byte("111"),
				[]byte{0x00, 0x00, 0x00, 0x00},
				dummyOpcode, encodedUint64(4), []byte("2222"),
				[]byte{0x00, 0x00, 0x00, 0x00},
				dummyOpcode, encodedUint64(4), []byte("3333"),
			),
			[]messageIndexEntry{
				newMessageIndexEntry(0, 1, 0),
				newMessageIndexEntry(16, 2, 0),
				newMessageIndexEntry(33, 3, 0),
			},
		},
		{
			"right message shorter",
			flatten(
				dummyOpcode, encodedUint64(4), []byte("2222"),
				dummyOpcode, encodedUint64(3), []byte("111"),
				dummyOpcode, encodedUint64(4), []byte("3333"),
			),
			[]messageIndexEntry{
				newMessageIndexEntry(0, 2, 0),
				newMessageIndexEntry(13, 1, 0),
				newMessageIndexEntry(25, 3, 0),
			},
			flatten(
				dummyOpcode, encodedUint64(3), []byte("111"),
				dummyOpcode, encodedUint64(4), []byte("2222"),
				dummyOpcode, encodedUint64(4), []byte("3333"),
			),
			[]messageIndexEntry{
				newMessageIndexEntry(0, 1, 0),
				newMessageIndexEntry(12, 2, 0),
				newMessageIndexEntry(25, 3, 0),
			},
		},
		{
			"multiple disorderings",
			flatten(
				dummyOpcode, encodedUint64(4), []byte("2222"),
				dummyOpcode, encodedUint64(3), []byte("111"),
				dummyOpcode, encodedUint64(2), []byte("44"),
				dummyOpcode, encodedUint64(4), []byte("3333"),
			),
			[]messageIndexEntry{
				newMessageIndexEntry(0, 2, 0),
				newMessageIndexEntry(13, 1, 0),
				newMessageIndexEntry(25, 4, 0),
				newMessageIndexEntry(36, 3, 0),
			},
			flatten(
				dummyOpcode, encodedUint64(3), []byte("111"),
				dummyOpcode, encodedUint64(4), []byte("2222"),
				dummyOpcode, encodedUint64(4), []byte("3333"),
				dummyOpcode, encodedUint64(2), []byte("44"),
			),
			[]messageIndexEntry{
				newMessageIndexEntry(0, 1, 0),
				newMessageIndexEntry(12, 2, 0),
				newMessageIndexEntry(25, 3, 0),
				newMessageIndexEntry(38, 4, 0),
			},
		},
		{
			"already sorted",
			flatten(
				dummyOpcode, encodedUint64(3), []byte("111"),
				dummyOpcode, encodedUint64(4), []byte("2222"),
				dummyOpcode, encodedUint64(4), []byte("3333"),
				dummyOpcode, encodedUint64(2), []byte("44"),
			),
			[]messageIndexEntry{
				newMessageIndexEntry(0, 1, 0),
				newMessageIndexEntry(12, 2, 0),
				newMessageIndexEntry(25, 3, 0),
				newMessageIndexEntry(38, 4, 0),
			},
			flatten(
				dummyOpcode, encodedUint64(3), []byte("111"),
				dummyOpcode, encodedUint64(4), []byte("2222"),
				dummyOpcode, encodedUint64(4), []byte("3333"),
				dummyOpcode, encodedUint64(2), []byte("44"),
			),
			[]messageIndexEntry{
				newMessageIndexEntry(0, 1, 0),
				newMessageIndexEntry(12, 2, 0),
				newMessageIndexEntry(25, 3, 0),
				newMessageIndexEntry(38, 4, 0),
			},
		},
		{
			"breaks ties on offset",
			flatten(
				dummyOpcode, encodedUint64(4), []byte("1111"),
				dummyOpcode, encodedUint64(4), []byte("2222"),
				dummyOpcode, encodedUint64(4), []byte("3333"),
			),
			[]messageIndexEntry{
				newMessageIndexEntry(0, 1, 0),
				newMessageIndexEntry(13, 2, 1),
				newMessageIndexEntry(26, 1, 0),
			},
			flatten(
				dummyOpcode, encodedUint64(4), []byte("1111"),
				dummyOpcode, encodedUint64(4), []byte("3333"),
				dummyOpcode, encodedUint64(4), []byte("2222"),
			),
			[]messageIndexEntry{
				newMessageIndexEntry(0, 1, 0),
				newMessageIndexEntry(13, 1, 0),
				newMessageIndexEntry(26, 2, 1),
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			sortChunk(nil, c.chunk, c.index)
			assert.Equal(t, c.outputChunk, c.chunk)
			assert.Equal(t, c.outputIndex, c.index)
		})
	}
}

func TestSwapSlices(t *testing.T) {
	cases := []struct {
		assertion  string
		input      []byte
		output     []byte
		leftstart  int
		leftend    int
		rightstart int
		rightend   int
	}{
		{
			"left and right same length",
			[]byte("aaaaxxxaaaayyyaaaa"), // cspell:disable-line
			[]byte("aaaayyyaaaaxxxaaaa"), // cspell:disable-line
			4, 4 + 3,
			11, 11 + 3,
		},
		{
			"left longer than right",
			[]byte("aaaaxxxxaaaayyyaaaa"), // cspell:disable-line
			[]byte("aaaayyyaaaaxxxxaaaa"), // cspell:disable-line
			4, 4 + 4,
			12, 12 + 3,
		},
		{
			"right longer than left",
			[]byte("aaaaxxxaaaayyyyaaaa"), // cspell:disable-line
			[]byte("aaaayyyyaaaaxxxaaaa"), // cspell:disable-line
			4, 4 + 3,
			11, 11 + 4,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			swapSlices(nil, c.input, c.leftstart, c.leftend, c.rightstart, c.rightend)
			assert.Equal(t, c.output, c.input)
		})
	}
}

func TestChunkSorting(t *testing.T) {
	msg1 := Message{
		ChannelID:   0,
		Sequence:    0,
		LogTime:     1,
		PublishTime: 0,
		Data:        []byte{},
	}
	msg2 := Message{
		ChannelID:   0,
		Sequence:    0,
		LogTime:     2,
		PublishTime: 0,
		Data:        []byte{},
	}
	msg3 := Message{
		ChannelID:   0,
		Sequence:    0,
		LogTime:     3,
		PublishTime: 0,
		Data:        []byte{},
	}
	cases := []struct {
		assertion      string
		inputMessages  []Message
		outputMessages []Message
	}{
		{
			"in sorted order",
			[]Message{msg1, msg2, msg3},
			[]Message{msg1, msg2, msg3},
		},
		{
			"disordered 1/2",
			[]Message{msg2, msg1, msg3},
			[]Message{msg1, msg2, msg3},
		},
		{
			"disordered 2/3",
			[]Message{msg1, msg3, msg2},
			[]Message{msg1, msg2, msg3},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf := &bytes.Buffer{}
			writer, err := NewWriter(buf, &WriterOptions{
				ChunkSize:         megabyte,
				Chunked:           true,
				Compression:       "",
				SortChunkMessages: true,
			})
			assert.Nil(t, err)
			assert.Nil(t, writer.WriteHeader(&Header{}))
			assert.Nil(t, writer.WriteSchema(&Schema{}))
			assert.Nil(t, writer.WriteChannel(&Channel{}))
			for _, msg := range c.inputMessages {
				assert.Nil(t, writer.WriteMessage(&msg))
			}
			assert.Nil(t, writer.Close())
			outputMessages := []Message{}
			lexer, err := NewLexer(buf, &LexerOptions{})
			assert.Nil(t, err)
			for {
				tokenType, token, err := lexer.Next(nil)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					t.Error(err)
				}
				if tokenType == TokenMessage {
					message, err := ParseMessage(token)
					assert.Nil(t, err)
					outputMessages = append(outputMessages, *message)
				}
			}
			assert.Equal(t, c.outputMessages, outputMessages)
		})
	}
}

func TestChunkedReadWrite(t *testing.T) {
	for _, compression := range []CompressionFormat{
		CompressionZSTD,
		CompressionLZ4,
		CompressionNone,
	} {
		t.Run(fmt.Sprintf("chunked file with %s compression", compression), func(t *testing.T) {
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
			assert.Equal(t, int(w.Offset()), buf.Len())
			lexer, err := NewLexer(buf)
			assert.Nil(t, err)
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
		Chunked:     true,
		ChunkSize:   20,
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
		Chunked:     true,
		ChunkSize:   kilobyte,
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
		CreateTime:  99,
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
			Offset:      38,
			Length:      67,
			LogTime:     100,
			CreateTime:  99,
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
		ChunkSize:   kilobyte,
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

func BenchmarkChunkSorting(b *testing.B) {
	cases := []struct {
		assertion      string
		rateDisordered float64
	}{
		{
			"zero disordering",
			0,
		},
		{
			"0.01% disordering",
			0.0001,
		},
		{
			"0.1% disordering",
			0.001,
		},
		{
			"1% disordering",
			0.01,
		},
		{
			"10% disordering",
			0.1,
		},
		{
			"50% disordering",
			0.5,
		},
	}
	buf := bytes.NewBuffer(make([]byte, 4*megabyte))
	for _, sortChunks := range []bool{false, true} {
		for _, c := range cases {
			b.ResetTimer()
			b.Run(fmt.Sprintf("%s sort chunks %v", c.assertion, sortChunks), func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					t0 := time.Now()
					writer, err := NewWriter(buf, &WriterOptions{
						ChunkSize:         kilobyte,
						Chunked:           true,
						SortChunkMessages: sortChunks,
					})
					assert.Nil(b, err)

					// write a million messages to the output
					assert.Nil(b, writer.WriteHeader(&Header{}))
					assert.Nil(b, writer.WriteSchema(&Schema{}))
					assert.Nil(b, writer.WriteChannel(&Channel{}))
					for i := uint64(1); i < 1e6+1; i += 2 {
						if rand.Float64() < c.rateDisordered {
							assert.Nil(b, writer.WriteMessage(&Message{
								LogTime: i,
							}))
							assert.Nil(b, writer.WriteMessage(&Message{
								LogTime: i - 1,
							}))
						} else {
							assert.Nil(b, writer.WriteMessage(&Message{
								LogTime: i - 1,
							}))
							assert.Nil(b, writer.WriteMessage(&Message{
								LogTime: i,
							}))
						}
					}
					assert.Nil(b, writer.Close())
					elapsed := time.Since(t0)
					b.ReportMetric(float64(1e6)/elapsed.Seconds(), "messages/second")
					buf.Reset()
				}
			})
		}
	}
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
			8 * megabyte,
			2e6,
			100,
		},
		{
			"small chunks many messages",
			8 * kilobyte,
			2e6,
			100,
		},
		{
			"many channels",
			4 * megabyte,
			2e6,
			55000,
		},
	}

	stringData := "hello, world!"
	messageData := []byte("hello, world")
	buf := bytes.NewBuffer(make([]byte, 4*gigabyte))
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
