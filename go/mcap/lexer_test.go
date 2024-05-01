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
	"github.com/stretchr/testify/require"
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
	r := bytes.NewReader(file)
	lexer, err := NewLexer(r)
	require.NoError(t, err)
	expectations := []struct {
		token      TokenType
		fileOffset uint64
	}{
		{TokenHeader, 8},
		{TokenChannel, 17},
		{TokenMessage, 26},
		{TokenMessage, 35},
		{TokenChannel, 134},
		{TokenAttachmentIndex, 143},
		{TokenFooter, 152},
	}
	for i, expected := range expectations {
		tokenType, _, err := lexer.Next(nil)
		require.NoError(t, err)
		assert.Equal(t, expected.token, tokenType)
		offset := lexer.GetLastTokenOffset()
		assert.Equal(t, uint64(0), offset.ChunkOffset)
		assert.Equal(t, expected.fileOffset, offset.FileOffset,
			fmt.Sprintf("expected file offset %d, got %d at index %d", expected.fileOffset, offset.FileOffset, i))
	}
}

func TestRejectsUnsupportedCompression(t *testing.T) {
	file := file(
		chunk(t, CompressionFormat("unknown"), true,
			chunk(t, CompressionZSTD, true, channelInfo(), message(), message())),
	)
	lexer, err := NewLexer(bytes.NewReader(file))
	require.NoError(t, err)
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
	require.NoError(t, err)
	_, _, err = lexer.Next(nil)
	require.ErrorIs(t, err, ErrRecordTooLarge)
}

func TestRejectsTooLargeChunks(t *testing.T) {
	bigChunk := chunk(t, CompressionZSTD, true, channelInfo(), message(), message())
	binary.LittleEndian.PutUint64(bigChunk[1+8+8+8:], 1000)
	file := file(header(), bigChunk, footer())
	lexer, err := NewLexer(bytes.NewReader(file), &LexerOptions{
		MaxDecompressedChunkSize: 999,
		ValidateChunkCRCs:        true,
	})
	require.NoError(t, err)
	_, _, err = lexer.Next(nil)
	require.NoError(t, err)
	_, _, err = lexer.Next(nil)
	require.ErrorIs(t, err, ErrChunkTooLarge)
}

func TestLargeChunksOKIfNotCheckingCRC(t *testing.T) {
	bigChunk := chunk(t, CompressionZSTD, true, channelInfo(), message(), message())
	binary.LittleEndian.PutUint64(bigChunk[1+8+8+8:], 1000)
	file := file(header(), bigChunk, footer())
	lexer, err := NewLexer(bytes.NewReader(file), &LexerOptions{
		MaxDecompressedChunkSize: 999,
	})
	require.NoError(t, err)
	_, _, err = lexer.Next(nil)
	require.NoError(t, err)
	_, _, err = lexer.Next(nil)
	require.NoError(t, err)
}

func TestRejectsNestedChunks(t *testing.T) {
	file := file(
		header(),
		chunk(t, CompressionZSTD, true, chunk(t, CompressionZSTD, true, channelInfo(), message(), message())),
		footer(),
	)
	lexer, err := NewLexer(bytes.NewReader(file))
	require.NoError(t, err)
	// header, then error
	tokenType, _, err := lexer.Next(nil)
	require.NoError(t, err)
	assert.Equal(t, TokenHeader, tokenType)
	_, _, err = lexer.Next(nil)
	require.ErrorIs(t, err, ErrNestedChunk)
}

func TestNewReaderBadMagic(t *testing.T) {
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
			require.ErrorIs(t, &ErrBadMagic{}, err)
		})
	}
}

type lzreader struct {
	*lz4.Reader
}

func (l lzreader) Reset(r io.Reader) error {
	l.Reader.Reset(r)
	return nil
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
	require.NoError(t, lzr.Apply(lz4.OnBlockDoneOption(func(int) {
		blockCount++
	})))
	lexer, err := NewLexer(bytes.NewReader(buf), &LexerOptions{
		Decompressors: map[CompressionFormat]ResettableReader{
			CompressionLZ4: lzreader{lzr},
		},
	})
	require.NoError(t, err)
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
		require.NoError(t, err)
		assert.Equal(t, expectedTokenType, tokenType, fmt.Sprintf("mismatch element %d", i))
	}
	assert.Positive(t, blockCount)
}

func TestReturnsEOFOnSuccessiveCalls(t *testing.T) {
	lexer, err := NewLexer(bytes.NewReader(file()))
	require.NoError(t, err)
	_, _, err = lexer.Next(nil)
	require.ErrorIs(t, err, io.EOF)
	_, _, err = lexer.Next(nil)
	require.ErrorIs(t, err, io.EOF)
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
					r := bytes.NewReader(file)
					lexer, err := NewLexer(r, &LexerOptions{ValidateChunkCRCs: validateCRC})
					require.NoError(t, err)
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
						require.NoError(t, err)
						assert.Equal(t, expectedTokenType, tokenType,
							fmt.Sprintf("expected %s but got %s at index %d", expectedTokenType, tokenType, i))
					}

					// now we are eof
					_, _, err = lexer.Next(nil)
					require.ErrorIs(t, err, io.EOF)
				})
			}
		})
	}
}
func TestOffsetsInChunkedFile(t *testing.T) {
	for _, validateCRC := range []bool{
		true,
		false,
	} {
		t.Run(fmt.Sprintf("crc validation %v", validateCRC), func(t *testing.T) {
			file := file(
				header(),
				chunk(t, CompressionNone, true, channelInfo(), message(), message()),
				chunk(t, CompressionNone, true, channelInfo(), message(), message()),
				attachment(), attachment(),
				footer(),
			)
			r := bytes.NewReader(file)
			lexer, err := NewLexer(r, &LexerOptions{ValidateChunkCRCs: validateCRC})
			require.NoError(t, err)
			expectations := []struct {
				token  TokenType
				offset RecordOffset
			}{
				{TokenHeader, RecordOffset{8, 0}},
				{TokenChannel, RecordOffset{17, 0}},
				{TokenMessage, RecordOffset{17, 9}},
				{TokenMessage, RecordOffset{17, 18}},
				{TokenChannel, RecordOffset{93, 0}},
				{TokenMessage, RecordOffset{93, 9}},
				{TokenMessage, RecordOffset{93, 18}},
				{TokenFooter, RecordOffset{259, 0}},
			}
			for i, expected := range expectations {
				tokenType, _, err := lexer.Next(nil)
				require.NoError(t, err)
				assert.Equal(t, expected.token, tokenType,
					fmt.Sprintf("expected token %s but got %s at index %d", expected.token, tokenType, i))
				offset := lexer.GetLastTokenOffset()
				assert.Equal(t, expected.offset.FileOffset, offset.FileOffset,
					fmt.Sprintf("expected file offset %d but got %d at index %d",
						expected.offset.FileOffset,
						offset.FileOffset,
						i))
				assert.Equal(t, expected.offset.ChunkOffset, offset.ChunkOffset,
					fmt.Sprintf("expected chunk offset %d but got %d at index %d",
						expected.offset.ChunkOffset,
						offset.ChunkOffset,
						i))
			}

			// now we are eof
			_, _, err = lexer.Next(nil)
			require.ErrorIs(t, err, io.EOF)
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
	require.NoError(t, err)
	expected := []TokenType{TokenHeader, TokenMessage}
	for i, expectedTokenType := range expected {
		tokenType, _, err := lexer.Next(nil)
		require.NoError(t, err)
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
		require.NoError(t, err)
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
			require.NoError(t, err)
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
		require.NoError(t, err)
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
			require.NoError(t, err)
			assert.Equal(t, expectedTokenType, tokenType, fmt.Sprintf("mismatch element %d", i))
		}
	})
	t.Run("validation fails on corrupted file", func(t *testing.T) {
		badchunk := chunk(t, CompressionZSTD, true, channelInfo(), message(), message())

		// chunk must be corrupted at a deep enough offset to hit the compressed data section
		require.NotEqual(t, 0x00, badchunk[35])
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
		require.NoError(t, err)
		expected := []TokenType{
			TokenHeader,
			TokenChannel,
			TokenMessage,
			TokenMessage,
		}
		for i, expectedTokenType := range expected {
			tokenType, _, err := lexer.Next(nil)
			require.NoError(t, err)
			assert.Equal(t, expectedTokenType, tokenType, fmt.Sprintf("mismatch element %d", i))
		}
		_, _, err = lexer.Next(nil)
		require.Error(t, err)
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
			require.NoError(t, err)
			require.NoError(t, writer.WriteHeader(&Header{
				Profile: "",
				Library: "",
			}))
			require.NoError(t, writer.WriteAttachment(c.attachment))
			require.NoError(t, writer.Close())

			var called bool
			lexer, err := NewLexer(file, &LexerOptions{
				ComputeAttachmentCRCs: true,
				AttachmentCallback: func(ar *AttachmentReader) error {
					assert.Equal(t, c.attachment.LogTime, ar.LogTime)
					assert.Equal(t, c.attachment.CreateTime, ar.CreateTime)
					assert.Equal(t, c.attachment.Name, ar.Name)
					assert.Equal(t, c.attachment.MediaType, ar.MediaType)
					assert.Equal(t, uint64(39), ar.Offset.FileOffset)
					assert.Equal(t, uint64(0), ar.Offset.ChunkOffset)
					data, err := io.ReadAll(ar.Data())
					require.NoError(t, err)
					assert.Equal(t, c.attachmentData, data)
					computedCRC, err := ar.ComputedCRC()
					require.NoError(t, err)
					parsedCRC, err := ar.ParsedCRC()
					require.NoError(t, err)
					assert.Equal(t, computedCRC, parsedCRC)
					called = true
					return nil
				},
			})

			for !errors.Is(err, io.EOF) {
				_, _, err = lexer.Next(nil)
				if !errors.Is(err, io.EOF) {
					require.NoError(t, err)
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
					require.NoError(t, err)
					expected := []TokenType{
						TokenHeader,
						TokenChunk,
						TokenChunk,
						TokenFooter,
					}
					for i, expectedTokenType := range expected {
						tokenType, _, err := lexer.Next(nil)
						require.NoError(t, err)
						assert.Equal(t, expectedTokenType, tokenType, fmt.Sprintf("mismatch element %d", i))
					}
					_, _, err = lexer.Next(nil)
					require.ErrorIs(t, err, io.EOF)
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
		require.NoError(b, err)
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
					require.NoError(b, err)
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

func TestBadMagic(t *testing.T) {
	t.Run("demonstrate usage of is", func(t *testing.T) {
		err := &ErrBadMagic{
			location: magicLocationStart,
			actual:   []byte{0x00, 0x01, 0x02, 0x03},
		}
		require.ErrorIs(t, err, &ErrBadMagic{})
	})
	t.Run("example of formatting", func(t *testing.T) {
		err := &ErrBadMagic{
			location: magicLocationStart,
			actual:   []byte{0x00, 0x01, 0x02, 0x03},
		}
		require.Equal(t, "Invalid magic at start of file, found: [0 1 2 3]", err.Error())
	})
}
