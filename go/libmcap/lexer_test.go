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

func TestLexUnchunkedFile(t *testing.T) {
	file := file(
		header(),
		channelInfo(),
		message(),
		message(),
		record(OpAttachment),
		record(OpAttachment),
		channelInfo(),
		record(OpAttachmentIndex),
		footer(),
	)
	lexer, err := NewLexer(bytes.NewReader(file))
	assert.Nil(t, err)
	expected := []TokenType{
		TokenHeader,
		TokenChannelInfo,
		TokenMessage,
		TokenMessage,
		TokenAttachment,
		TokenAttachment,
		TokenChannelInfo,
		TokenAttachmentIndex,
		TokenFooter,
	}
	for _, tt := range expected {
		tk, err := lexer.Next()
		assert.Nil(t, err)
		tk.bytes()
		assert.Equal(t, tt, tk.TokenType)
	}
}

func TestRejectsUnsupportedCompression(t *testing.T) {
	file := file(
		chunk(t, CompressionFormat("unknown"), chunk(t, CompressionLZ4, channelInfo(), message(), message())),
	)
	lexer, err := NewLexer(bytes.NewReader(file))
	assert.Nil(t, err)
	_, err = lexer.Next()
	assert.Equal(t, "unsupported compression: unknown", err.Error())
}

func TestRejectsNestedChunks(t *testing.T) {
	file := file(
		header(),
		chunk(t, CompressionLZ4, chunk(t, CompressionLZ4, channelInfo(), message(), message())),
		footer(),
	)
	lexer, err := NewLexer(bytes.NewReader(file))
	assert.Nil(t, err)
	// header, then error
	tk, err := lexer.Next()
	assert.Nil(t, err)
	assert.Equal(t, tk.TokenType, TokenHeader)
	tk, err = lexer.Next()
	assert.ErrorIs(t, ErrNestedChunk, err)
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
			assert.ErrorIs(t, err, ErrBadMagic)
		})
	}
}

func TestShortMagicResultsCorrectError(t *testing.T) {
	_, err := NewLexer(bytes.NewReader(make([]byte, 4)))
	assert.ErrorIs(t, err, ErrBadMagic)
}

func TestReturnsEOFOnSuccessiveCalls(t *testing.T) {
	lexer, err := NewLexer(bytes.NewReader(file()))
	assert.Nil(t, err)
	_, err = lexer.Next()
	assert.ErrorIs(t, err, io.EOF)
	_, err = lexer.Next()
	assert.ErrorIs(t, err, io.EOF)
}

func TestLexChunkedFile(t *testing.T) {
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
						chunk(t, compression, channelInfo(), message(), message()),
						chunk(t, compression, channelInfo(), message(), message()),
						attachment(), attachment(),
						footer(),
					)
					lexer, err := NewLexer(bytes.NewReader(file), &LexOpts{
						ValidateCRC: validateCRC,
					})
					assert.Nil(t, err)
					expected := []TokenType{
						TokenHeader,
						TokenChannelInfo,
						TokenMessage,
						TokenMessage,
						TokenChannelInfo,
						TokenMessage,
						TokenMessage,
						TokenAttachment,
						TokenAttachment,
						TokenFooter,
					}
					for i, tt := range expected {
						tk, err := lexer.Next()
						tk.bytes()
						assert.Nil(t, err)
						assert.Equal(t, tt, tk.TokenType, fmt.Sprintf("expected %s but got %s at index %d", tt, tk, i))
					}

					// now we are eof
					tk, err := lexer.Next()
					tk.bytes()
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
	for i, tt := range expected {
		tk, _ := lexer.Next()
		_ = tk.bytes()
		assert.Equal(t, tt, tk.TokenType, fmt.Sprintf("mismatch element %d", i))
	}
}

func TestChunkCRCValidation(t *testing.T) {
	t.Run("validates valid file", func(t *testing.T) {
		file := file(
			header(),
			chunk(t, CompressionLZ4, channelInfo(), message(), message()),
			chunk(t, CompressionLZ4, channelInfo(), message(), message()),
			attachment(), attachment(),
			footer(),
		)
		lexer, err := NewLexer(bytes.NewReader(file), &LexOpts{
			ValidateCRC: true,
		})
		assert.Nil(t, err)
		expected := []TokenType{
			TokenHeader,
			TokenChannelInfo,
			TokenMessage,
			TokenMessage,
			TokenChannelInfo,
			TokenMessage,
			TokenMessage,
			TokenAttachment,
			TokenAttachment,
			TokenFooter,
		}
		for i, tt := range expected {
			tk, err := lexer.Next()
			assert.Nil(t, err)
			_ = tk.bytes() // always must consume the reader
			assert.Equal(t, tt, tk.TokenType, fmt.Sprintf("mismatch element %d", i))
		}
	})
	t.Run("validation fails on corrupted file", func(t *testing.T) {
		badchunk := chunk(t, CompressionLZ4, channelInfo(), message(), message())

		// chunk must be corrupted at a deep enough offset to hit the compressed data section
		assert.NotEqual(t, badchunk[35], 0x00)
		badchunk[35] = 0x00
		file := file(
			header(),
			chunk(t, CompressionLZ4, channelInfo(), message(), message()),
			badchunk,
			attachment(), attachment(),
			footer(),
		)
		lexer, err := NewLexer(bytes.NewReader(file), &LexOpts{
			ValidateCRC: true,
		})
		assert.Nil(t, err)
		expected := []TokenType{
			TokenHeader,
			TokenChannelInfo,
			TokenMessage,
			TokenMessage,
		}
		for i, tt := range expected {
			tk, err := lexer.Next()
			assert.Nil(t, err)
			_ = tk.bytes() // always must consume the reader
			assert.Equal(t, tt, tk.TokenType, fmt.Sprintf("mismatch element %d", i))
		}
		_, err = lexer.Next()
		assert.NotNil(t, err)
		assert.Equal(t, "invalid CRC: ffaaf97a != ff00f97a", err.Error())
	})
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
						chunk(t, compression, channelInfo(), message(), message()),
						chunk(t, compression, channelInfo(), message(), message()),
						attachment(), attachment(),
						footer(),
					)
					lexer, err := NewLexer(bytes.NewReader(file), &LexOpts{
						ValidateCRC: validateCRC,
						EmitChunks:  true,
					})
					assert.Nil(t, err)
					expected := []TokenType{
						TokenHeader,
						TokenChunk,
						TokenChunk,
						TokenAttachment,
						TokenAttachment,
						TokenFooter,
					}
					for i, tt := range expected {
						tk, err := lexer.Next()
						assert.Nil(t, err)
						_ = tk.bytes() // always must consume the reader
						assert.Equal(t, tt, tk.TokenType, fmt.Sprintf("mismatch element %d", i))
					}
					_, err = lexer.Next()
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
		//{
		//	"cal_loop.bag",
		//	"../../testdata/cal_loop.mcap",
		//},
		//{
		//	"turtlebot.bag",
		//	"../../testdata/turtlebot3-burger-2021-04-22-15-35-44.mcap",
		//},
	}
	for _, c := range cases {
		input, err := os.ReadFile(c.inputfile)
		assert.Nil(b, err)
		reader := &bytes.Reader{}
		b.ResetTimer()
		msg := make([]byte, 1024*1024)
		b.Run(c.assertion, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				t0 := time.Now()
				var tokens, bytecount int64
				reader.Reset(input)
				lexer, err := NewLexer(reader)
				assert.Nil(b, err)
				for {
					tok, err := lexer.Next()
					if errors.Is(err, io.EOF) {
						break
					}
					if int64(len(msg)) < tok.ByteCount {
						msg = make([]byte, tok.ByteCount)
					}
					n, err := tok.Reader.Read(msg[:tok.ByteCount])
					if err != nil {
						b.Errorf("parse fail: %s", err)
					}
					tokens++
					bytecount += int64(n)
				}
				elapsed := time.Since(t0)
				mbread := bytecount / (1024 * 1024)
				b.ReportMetric(float64(mbread)/elapsed.Seconds(), "MB/sec")
				b.ReportMetric(float64(tokens)/elapsed.Seconds(), "tokens/sec")
				b.ReportMetric(float64(elapsed.Nanoseconds())/float64(tokens), "ns/token")
			}
		})
	}
}
