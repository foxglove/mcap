package mcap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
)

func TestLexUnchunkedFile(t *testing.T) {
	file := file(
		header(),
		channelInfo(),
		message(),
		message(),
		attachment(),
		attachment(),
		channelInfo(),
		record(OpAttachmentIndex),
		footer(),
	)
	lexer, err := NewLexer(bytes.NewReader(file))
	assert.Nil(t, err)
	expected := []TokenType{
		TokenHeader,
		TokenChannel,
		TokenMessage,
		TokenMessage,
		TokenChannel,
		TokenAttachmentIndex,
		TokenFooter,
	}
	for _, expectedTokenType := range expected {
		tokenType, _, err := lexer.Next(nil)
		assert.Nil(t, err)
		assert.Equal(t, expectedTokenType, tokenType)
	}
}

func TestRejectsUnsupportedCompression(t *testing.T) {
	file := file(
		chunk(t, CompressionFormat("unknown"), true,
			chunk(t, CompressionZSTD, true, channelInfo(), message(), message())),
	)
	lexer, err := NewLexer(bytes.NewReader(file))
	assert.Nil(t, err)
	_, _, err = lexer.Next(nil)
	assert.Equal(t, "unsupported compression: unknown", err.Error())
}

func TestRejectsTooLargeRecords(t *testing.T) {
	bigHeader := header()
	binary.LittleEndian.PutUint64(bigHeader[1:], 1000)
	file := file(bigHeader)
	lexer, err := NewLexer(bytes.NewReader(file), &LexerOptions{
		MaxRecordSize: 999,
	})
	assert.Nil(t, err)
	_, _, err = lexer.Next(nil)
	assert.ErrorIs(t, err, ErrRecordTooLarge)
}

func TestRejectsTooLargeChunks(t *testing.T) {
	bigChunk := chunk(t, CompressionZSTD, true, channelInfo(), message(), message())
	binary.LittleEndian.PutUint64(bigChunk[1+8+8+8:], 1000)
	file := file(header(), bigChunk, footer())
	lexer, err := NewLexer(bytes.NewReader(file), &LexerOptions{
		MaxDecompressedChunkSize: 999,
		ValidateChunkCRCs:        true,
	})
	assert.Nil(t, err)
	_, _, err = lexer.Next(nil)
	assert.Nil(t, err)
	_, _, err = lexer.Next(nil)
	assert.ErrorIs(t, err, ErrChunkTooLarge)
}

func TestLargeChunksOKIfNotCheckingCRC(t *testing.T) {
	bigChunk := chunk(t, CompressionZSTD, true, channelInfo(), message(), message())
	binary.LittleEndian.PutUint64(bigChunk[1+8+8+8:], 1000)
	file := file(header(), bigChunk, footer())
	lexer, err := NewLexer(bytes.NewReader(file), &LexerOptions{
		MaxDecompressedChunkSize: 999,
	})
	assert.Nil(t, err)
	_, _, err = lexer.Next(nil)
	assert.Nil(t, err)
	_, _, err = lexer.Next(nil)
	assert.Nil(t, err)
}

func TestRejectsNestedChunks(t *testing.T) {
	file := file(
		header(),
		chunk(t, CompressionZSTD, true, chunk(t, CompressionZSTD, true, channelInfo(), message(), message())),
		footer(),
	)
	lexer, err := NewLexer(bytes.NewReader(file))
	assert.Nil(t, err)
	// header, then error
	tokenType, _, err := lexer.Next(nil)
	assert.Nil(t, err)
	assert.Equal(t, tokenType, TokenHeader)
	_, _, err = lexer.Next(nil)
	assert.ErrorIs(t, err, ErrNestedChunk)
}

func TestBadMagic(t *testing.T) {
	cases := []struct {
		assertion string
		magic     []byte
	}{
		{
			"short magic",
			make([]byte, 4),
		},
		{
			"invalid magic",
			make([]byte, 20),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			_, err := NewLexer(bytes.NewReader(c.magic))
			assert.IsType(t, &ErrBadMagic{}, err)
		})
	}
}

func TestCustomDecompressor(t *testing.T) {
	buf := file(
		header(),
		chunk(t, CompressionLZ4, true, channelInfo(), message(), message()),
		chunk(t, CompressionLZ4, true, channelInfo(), message(), message()),
		attachment(), attachment(),
		footer(),
	)
	lzr := lz4.NewReader(nil)
	blockCount := 0
	lzr.Apply(lz4.OnBlockDoneOption(func(size int) {
		blockCount++
	}))
	lexer, err := NewLexer(bytes.NewReader(buf), &LexerOptions{
		Decompressors: map[CompressionFormat]ResettableReader{
			CompressionLZ4: lzr,
		},
	})
	assert.Nil(t, err)
	expected := []TokenType{
		TokenHeader,
		TokenChannel,
		TokenMessage,
		TokenMessage,
		TokenChannel,
		TokenMessage,
		TokenMessage,
		TokenFooter,
	}
	for i, expectedTokenType := range expected {
		tokenType, _, err := lexer.Next(nil)
		assert.Nil(t, err)
		assert.Equal(t, expectedTokenType, tokenType, fmt.Sprintf("mismatch element %d", i))
	}
	assert.Positive(t, blockCount)
}

func TestReturnsEOFOnSuccessiveCalls(t *testing.T) {
	lexer, err := NewLexer(bytes.NewReader(file()))
	assert.Nil(t, err)
	_, _, err = lexer.Next(nil)
	assert.ErrorIs(t, err, io.EOF)
	_, _, err = lexer.Next(nil)
	assert.ErrorIs(t, err, io.EOF)
}

func TestLexChunkedFile(t *testing.T) {
	for _, validateCRC := range []bool{
		true,
		false,
	} {
		t.Run(fmt.Sprintf("crc validation %v", validateCRC), func(t *testing.T) {
			for _, compression := range []CompressionFormat{
				CompressionZSTD,
				CompressionLZ4,
				CompressionNone,
			} {
				t.Run(fmt.Sprintf("chunked %s", compression), func(t *testing.T) {
					file := file(
						header(),
						chunk(t, compression, true, channelInfo(), message(), message()),
						chunk(t, compression, true, channelInfo(), message(), message()),
						attachment(), attachment(),
						footer(),
					)
					lexer, err := NewLexer(bytes.NewReader(file), &LexerOptions{
						ValidateChunkCRCs: validateCRC,
					})
					assert.Nil(t, err)
					expected := []TokenType{
						TokenHeader,
						TokenChannel,
						TokenMessage,
						TokenMessage,
						TokenChannel,
						TokenMessage,
						TokenMessage,
						TokenFooter,
					}
					for i, expectedTokenType := range expected {
						tokenType, _, err := lexer.Next(nil)
						assert.Nil(t, err)
						assert.Equal(t, expectedTokenType, tokenType,
							fmt.Sprintf("expected %s but got %s at index %d", expectedTokenType, tokenType, i))
					}

					// now we are eof
					_, _, err = lexer.Next(nil)
					assert.ErrorIs(t, err, io.EOF)
				})
			}
		})
	}
}

func TestSkipsUnknownOpcodes(t *testing.T) {
	unrecognized := make([]byte, 9)
	unrecognized[0] = 0x99 // zero-length unknown record
	file := file(
		header(),
		unrecognized,
		message(),
	)
	lexer, err := NewLexer(bytes.NewReader(file))
	assert.Nil(t, err)
	expected := []TokenType{TokenHeader, TokenMessage}
	for i, expectedTokenType := range expected {
		tokenType, _, err := lexer.Next(nil)
		assert.Nil(t, err)
		assert.Equal(t, expectedTokenType, tokenType, fmt.Sprintf("mismatch element %d", i))
	}
}

func TestChunkCRCValidation(t *testing.T) {
	t.Run("validates valid file", func(t *testing.T) {
		file := file(
			header(),
			chunk(t, CompressionZSTD, true, channelInfo(), message(), message()),
			chunk(t, CompressionZSTD, true, channelInfo(), message(), message()),
			attachment(), attachment(),
			footer(),
		)
		lexer, err := NewLexer(bytes.NewReader(file), &LexerOptions{
			ValidateChunkCRCs: true,
		})
		assert.Nil(t, err)
		expected := []TokenType{
			TokenHeader,
			TokenChannel,
			TokenMessage,
			TokenMessage,
			TokenChannel,
			TokenMessage,
			TokenMessage,
			TokenFooter,
		}
		for i, expectedTokenType := range expected {
			tokenType, _, err := lexer.Next(nil)
			assert.Nil(t, err)
			assert.Equal(t, expectedTokenType, tokenType, fmt.Sprintf("mismatch element %d", i))
		}
	})
	t.Run("validates file with zero'd CRCs", func(t *testing.T) {
		file := file(
			header(),
			chunk(t, CompressionZSTD, false, channelInfo(), message(), message()),
			chunk(t, CompressionZSTD, false, channelInfo(), message(), message()),
			attachment(), attachment(),
			footer(),
		)
		lexer, err := NewLexer(bytes.NewReader(file), &LexerOptions{
			ValidateChunkCRCs: true,
		})
		assert.Nil(t, err)
		expected := []TokenType{
			TokenHeader,
			TokenChannel,
			TokenMessage,
			TokenMessage,
			TokenChannel,
			TokenMessage,
			TokenMessage,
			TokenFooter,
		}
		for i, expectedTokenType := range expected {
			tokenType, _, err := lexer.Next(nil)
			assert.Nil(t, err)
			assert.Equal(t, expectedTokenType, tokenType, fmt.Sprintf("mismatch element %d", i))
		}
	})
	t.Run("validation fails on corrupted file", func(t *testing.T) {
		badchunk := chunk(t, CompressionZSTD, true, channelInfo(), message(), message())

		// chunk must be corrupted at a deep enough offset to hit the compressed data section
		assert.NotEqual(t, badchunk[35], 0x00)
		badchunk[35] = 0x00
		file := file(
			header(),
			chunk(t, CompressionZSTD, true, channelInfo(), message(), message()),
			badchunk,
			attachment(), attachment(),
			footer(),
		)
		lexer, err := NewLexer(bytes.NewReader(file), &LexerOptions{
			ValidateChunkCRCs: true,
		})
		assert.Nil(t, err)
		expected := []TokenType{
			TokenHeader,
			TokenChannel,
			TokenMessage,
			TokenMessage,
		}
		for i, expectedTokenType := range expected {
			tokenType, _, err := lexer.Next(nil)
			assert.Nil(t, err)
			assert.Equal(t, expectedTokenType, tokenType, fmt.Sprintf("mismatch element %d", i))
		}
		_, _, err = lexer.Next(nil)
		assert.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), "invalid chunk CRC"))
	})
}

func TestAttachmentHandling(t *testing.T) {
	cases := []struct {
		assertion      string
		attachmentData []byte
		attachment     *Attachment
	}{
		{
			"empty attachment",
			[]byte{},
			&Attachment{
				LogTime:    0,
				CreateTime: 0,
				Name:       "empty",
				MediaType:  "mediaType",
				DataSize:   0,
			},
		},
		{
			"nonempty attachment",
			[]byte{0x01, 0x02, 0x03, 0x04},
			&Attachment{
				LogTime:    0,
				CreateTime: 0,
				Name:       "nonempty",
				MediaType:  "media",
				DataSize:   4,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			c.attachment.Data = bytes.NewReader(c.attachmentData)
			file := &bytes.Buffer{}
			writer, err := NewWriter(file, &WriterOptions{})
			assert.Nil(t, err)
			assert.Nil(t, writer.WriteHeader(&Header{
				Profile: "",
				Library: "",
			}))
			assert.Nil(t, writer.WriteAttachment(c.attachment))
			assert.Nil(t, writer.Close())

			var called bool
			lexer, err := NewLexer(file, &LexerOptions{
				ComputeAttachmentCRCs: true,
				AttachmentCallback: func(ar *AttachmentReader) error {
					assert.Equal(t, c.attachment.LogTime, ar.LogTime)
					assert.Equal(t, c.attachment.CreateTime, ar.CreateTime)
					assert.Equal(t, c.attachment.Name, ar.Name)
					assert.Equal(t, c.attachment.MediaType, ar.MediaType)
					data, err := io.ReadAll(ar.Data())
					assert.Nil(t, err)
					assert.Equal(t, c.attachmentData, data)
					computedCRC, err := ar.ComputedCRC()
					assert.Nil(t, err)
					parsedCRC, err := ar.ParsedCRC()
					assert.Nil(t, err)
					assert.Equal(t, computedCRC, parsedCRC)
					called = true
					return nil
				},
			})

			for !errors.Is(err, io.EOF) {
				_, _, err = lexer.Next(nil)
				if !errors.Is(err, io.EOF) {
					assert.Nil(t, err)
				}
			}
			assert.True(t, called)
		})
	}
}

func TestChunkEmission(t *testing.T) {
	for _, validateCRC := range []bool{
		true,
		false,
	} {
		t.Run(fmt.Sprintf("crc validation %v", validateCRC), func(t *testing.T) {
			for _, compression := range []CompressionFormat{
				CompressionLZ4,
				CompressionZSTD,
				CompressionNone,
			} {
				t.Run(fmt.Sprintf("chunked %s", compression), func(t *testing.T) {
					file := file(
						header(),
						chunk(t, compression, true, channelInfo(), message(), message()),
						chunk(t, compression, true, channelInfo(), message(), message()),
						attachment(), attachment(),
						footer(),
					)
					lexer, err := NewLexer(bytes.NewReader(file), &LexerOptions{
						ValidateChunkCRCs: validateCRC,
						EmitChunks:        true,
					})
					assert.Nil(t, err)
					expected := []TokenType{
						TokenHeader,
						TokenChunk,
						TokenChunk,
						TokenFooter,
					}
					for i, expectedTokenType := range expected {
						tokenType, _, err := lexer.Next(nil)
						assert.Nil(t, err)
						assert.Equal(t, expectedTokenType, tokenType, fmt.Sprintf("mismatch element %d", i))
					}
					_, _, err = lexer.Next(nil)
					assert.ErrorIs(t, err, io.EOF)
				})
			}
		})
	}
}

func BenchmarkLexer(b *testing.B) {
	cases := []struct {
		assertion string
		inputfile string
	}{
		{
			"demo.bag",
			"../../testdata/mcap/demo.mcap",
		},
		// {
		// 	"cal_loop.bag",
		// 	"../../testdata/cal_loop.mcap",
		// },
		// {
		// 	"turtlebot.bag",
		// 	"../../testdata/turtlebot3-burger-2021-04-22-15-35-44.mcap",
		// },
	}
	for _, c := range cases {
		input, err := os.ReadFile(c.inputfile)
		assert.Nil(b, err)
		reader := &bytes.Reader{}
		msg := make([]byte, 3*1024*1024)
		for _, validateCRC := range []bool{true, false} {
			b.ResetTimer()
			b.Run(fmt.Sprintf("%s - crc validation %v", c.assertion, validateCRC), func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					t0 := time.Now()
					var tokens, bytecount int64
					reader.Reset(input)
					lexer, err := NewLexer(reader, &LexerOptions{
						ValidateChunkCRCs: validateCRC,
					})
					assert.Nil(b, err)
					for {
						_, record, err := lexer.Next(msg)
						if errors.Is(err, io.EOF) {
							break
						}
						tokens++
						bytecount += int64(len(record))
					}
					elapsed := time.Since(t0)
					mbRead := bytecount / (1024 * 1024)
					b.ReportMetric(float64(mbRead)/elapsed.Seconds(), "MB/sec")
					b.ReportMetric(float64(tokens)/elapsed.Seconds(), "tokens/sec")
					b.ReportMetric(float64(elapsed.Nanoseconds())/float64(tokens), "ns/token")
				}
			})
		}
	}
}
