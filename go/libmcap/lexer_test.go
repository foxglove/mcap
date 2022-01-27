package libmcap

import (
	"bytes"
	"fmt"
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
		attachment(),
		attachment(),
		footer(),
	)
	lexer := NewLexer(bytes.NewReader(file))
	expected := []TokenType{
		TokenHeader,
		TokenChannelInfo,
		TokenMessage,
		TokenMessage,
		TokenAttachment,
		TokenAttachment,
		TokenFooter,
	}
	for _, tt := range expected {
		tk := lexer.Next()
		assert.Equal(t, tt, tk.TokenType)
	}
}

func TestRejectsUnsupportedCompression(t *testing.T) {
	file := file(
		chunk(t, CompressionFormat("unknown"), chunk(t, CompressionLZ4, channelInfo(), message(), message())),
	)
	lexer := NewLexer(bytes.NewReader(file))
	token := lexer.Next()
	assert.Equal(t, TokenError, token.TokenType)
	assert.Equal(t, "unsupported compression: unknown", string(token.bytes()))
}

func TestRejectsNestedChunks(t *testing.T) {
	file := file(
		header(),
		chunk(t, CompressionLZ4, chunk(t, CompressionLZ4, channelInfo(), message(), message())),
		footer(),
	)
	lexer := NewLexer(bytes.NewReader(file))
	expected := []TokenType{
		TokenHeader,
		TokenError,
	}
	var tk Token
	for _, tt := range expected {
		tk = lexer.Next()
		assert.Equal(t, tt, tk.TokenType)
	}
	assert.Equal(t, ErrNestedChunk.Error(), string(tk.bytes()))
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
			lexer := NewLexer(bytes.NewReader(c.magic))
			tk := lexer.Next()
			assert.Equal(t, TokenError, tk.TokenType)
			assert.Equal(t, ErrBadMagic.Error(), string(tk.bytes()))
		})
	}
}

func TestShortMagicResultsCorrectError(t *testing.T) {
	lexer := NewLexer(bytes.NewReader(make([]byte, 4)))
	tk := lexer.Next()
	assert.Equal(t, TokenError, tk.TokenType)
	assert.Equal(t, ErrBadMagic.Error(), string(tk.bytes()))
}

func TestReturnsEOFOnSuccessiveCalls(t *testing.T) {
	lexer := NewLexer(bytes.NewReader(file()))
	tk := lexer.Next()
	assert.Equal(t, TokenEOF, tk.TokenType)
	tk = lexer.Next()
	assert.Equal(t, TokenEOF, tk.TokenType)
}

func TestLexChunkedFile(t *testing.T) {
	for _, validateCRC := range []bool{true, false} {
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
					lexer := NewLexer(bytes.NewReader(file), &lexOpts{
						validateCRC: validateCRC,
					})
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
						TokenEOF,
					}
					for i, tt := range expected {
						tk := lexer.Next()
						assert.Equal(t, tt, tk.TokenType, fmt.Sprintf("mismatch element %d", i))
					}
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
	lexer := NewLexer(bytes.NewReader(file))
	expected := []TokenType{TokenHeader, TokenMessage}
	for i, tt := range expected {
		tk := lexer.Next()
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
		lexer := NewLexer(bytes.NewReader(file), &lexOpts{
			validateCRC: true,
		})
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
			TokenEOF,
		}
		for i, tt := range expected {
			tk := lexer.Next()
			_ = tk.bytes() // always must consume the reader
			assert.Equal(t, tt, tk.TokenType, fmt.Sprintf("mismatch element %d", i))
		}
	})
	t.Run("validation fails on corrupted file", func(t *testing.T) {
		badchunk := chunk(t, CompressionLZ4, channelInfo(), message(), message())
		badchunk[20] = 0x00 // corrupt the CRC
		file := file(
			header(),
			chunk(t, CompressionLZ4, channelInfo(), message(), message()),
			badchunk,
			attachment(), attachment(),
			footer(),
		)
		lexer := NewLexer(bytes.NewReader(file), &lexOpts{
			validateCRC: true,
		})
		expected := []TokenType{
			TokenHeader,
			TokenChannelInfo,
			TokenMessage,
			TokenMessage,
			TokenError,
		}
		for i, tt := range expected {
			tk := lexer.Next()
			data := tk.bytes() // always must consume the reader
			if tt == TokenError {
				assert.Equal(t, "invalid CRC: ffaaf97a != aaf97a", string(data))
			}
			assert.Equal(t, tt, tk.TokenType, fmt.Sprintf("mismatch element %d", i))
		}
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
					lexer := NewLexer(bytes.NewReader(file), &lexOpts{
						validateCRC: validateCRC,
						emitChunks:  true,
					})
					expected := []TokenType{
						TokenHeader,
						TokenChunk,
						TokenChunk,
						TokenAttachment,
						TokenAttachment,
						TokenFooter,
						TokenEOF,
					}
					for i, tt := range expected {
						tk := lexer.Next()
						_ = tk.bytes() // always must consume the reader
						assert.Equal(t, tt, tk.TokenType, fmt.Sprintf("mismatch element %d", i))
					}
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
				lexer := NewLexer(reader)
				for {
					tok := lexer.Next()
					if tok.TokenType == TokenEOF {
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
