package mcap

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type msg struct {
	logTime   uint64
	channelID uint16
}

func a(logTime uint64) msg {
	return msg{logTime: logTime, channelID: 1}
}
func b(logTime uint64) msg {
	return msg{logTime: logTime, channelID: 2}
}

func makeMcap(t *testing.T, messages []msg) []byte {
	buf := bytes.NewBuffer(nil)
	// expect 5 messages to fit per chunk, even if two schema and channel records have been written to
	// the chunk also.
	w, err := NewWriter(buf, &WriterOptions{
		ChunkSize:   500,
		Chunked:     true,
		Compression: CompressionNone,
		IncludeCRC:  true,
	})
	assert.Nil(t, err)
	wroteChannelA := false
	wroteChannelB := false
	msgData := make([]byte, 50)
	assert.Nil(t, w.WriteHeader(&Header{}))
	for i, msg := range messages {
		if msg.channelID == 1 && !wroteChannelA {
			assert.Nil(t, w.WriteSchema(&Schema{
				ID:       1,
				Name:     "schema_a",
				Encoding: "jsonschema",
				Data:     []byte("{}"),
			}))
			assert.Nil(t, w.WriteChannel(&Channel{
				ID:              1,
				Topic:           "a",
				MessageEncoding: "json",
				SchemaID:        1,
			}))
			wroteChannelA = true
		}
		if msg.channelID == 2 && !wroteChannelB {
			assert.Nil(t, w.WriteSchema(&Schema{
				ID:       2,
				Name:     "schema_b",
				Encoding: "jsonschema",
				Data:     []byte("{}"),
			}))
			assert.Nil(t, w.WriteChannel(&Channel{
				ID:              2,
				Topic:           "a",
				MessageEncoding: "json",
				SchemaID:        2,
			}))
			wroteChannelB = true
		}
		assert.Nil(t, w.WriteMessage(&Message{
			ChannelID:   msg.channelID,
			Sequence:    uint32(i),
			LogTime:     msg.logTime,
			PublishTime: msg.logTime,
			Data:        msgData,
		}))
	}
	assert.Nil(t, w.Close())

	return buf.Bytes()
}

func getInfo(t assert.TestingT, rs io.ReadSeeker) *Info {
	reader, err := NewReader(rs)
	assert.Nil(t, err)
	info, err := reader.Info()
	assert.Nil(t, err)
	_, err = rs.Seek(0, io.SeekStart)
	assert.Nil(t, err)
	return info
}

func TestReadsInOrder(t *testing.T) {
	cases := []struct {
		assertion string
		messages  []msg
	}{
		{
			assertion: "in-order file stays in order",
			messages:  []msg{a(1), a(2), a(3), a(4), a(5), a(6), a(7)},
		},
		{
			assertion: "resilient to some disordering",
			messages:  []msg{a(2), a(1), a(4), a(3), a(7), a(6), a(5)},
		},
		{
			assertion: "totally backwards",
			messages:  []msg{a(7), a(6), a(5), a(4), a(3), a(2), a(1)},
		},
		{
			assertion: "later channel first",
			messages:  []msg{a(4), a(5), a(6), a(7), a(8), a(9), b(1), b(2), b(3)},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			testfile := makeMcap(t, c.messages)
			rs := bytes.NewReader(testfile)
			info := getInfo(t, rs)
			schemaASeen := false
			schemaBSeen := false
			channelASeen := false
			channelBSeen := false
			sortedMessages := make([]msg, len(c.messages))
			copy(sortedMessages, c.messages)
			sort.Slice(sortedMessages, func(i, j int) bool {
				return sortedMessages[i].logTime < sortedMessages[j].logTime
			})
			reader, err := NewOrderedLexer(rs, info, 1024, nil)
			assert.Nil(t, err)

			buf := make([]byte, 1024)
			curMessageIndex := 0
			for {
				tkn, content, err := reader.Next(buf)
				if errors.Is(err, io.EOF) {
					break
				}
				assert.Nil(t, err)
				switch tkn {
				case TokenSchema:
					schema, err := ParseSchema(content)
					assert.Nil(t, err)
					switch schema.ID {
					case 1:
						schemaASeen = true
					case 2:
						schemaBSeen = true
					default:
						assert.Fail(t, "unexpected schema ID %d", schema.ID)
					}
				case TokenChannel:
					channel, err := ParseChannel(content)
					assert.Nil(t, err)
					switch channel.ID {
					case 1:
						assert.True(t, schemaASeen)
						channelASeen = true
					case 2:
						assert.True(t, schemaBSeen)
						channelBSeen = true
					default:
						assert.Fail(t, "unexpected channel ID %d", channel.ID)
					}
				case TokenMessage:
					expectedMessage := sortedMessages[curMessageIndex]
					curMessageIndex++
					msg, err := ParseMessage(content)
					assert.Nil(t, err)
					assert.Equal(t, expectedMessage.channelID, msg.ChannelID)
					assert.Equal(t, expectedMessage.logTime, msg.LogTime)
					switch msg.ChannelID {
					case 1:
						assert.True(t, channelASeen)
					case 2:
						assert.True(t, channelBSeen)
					default:
						assert.Fail(t, "unexpected channel id %d", msg.ChannelID)
					}
				}
			}
			assert.Equal(t, curMessageIndex, len(c.messages))
		})
	}
}

func TestGuardsMemoryUse(t *testing.T) {
	cases := []struct {
		assertion     string
		minBufferSize uint64
		messages      []msg
	}{
		{
			assertion:     "two chunks in order",
			minBufferSize: 559,
			messages:      []msg{a(1), a(2), a(3), a(4), a(5), a(6), a(7)},
		},
		{
			assertion:     "disordered messages",
			minBufferSize: 875,
			messages:      []msg{a(4), a(5), a(6), a(7), a(8), a(9), b(1), b(2), b(3)},
		},
		{
			assertion:     "many chunks, last contains early messages",
			minBufferSize: 1936,
			messages: []msg{
				a(10), a(11), a(12), a(13), a(14), a(15), a(16), a(17), a(18), a(19),
				a(20), a(21), a(22), a(23), a(24), a(25), a(26), a(27), a(28), a(29),
				a(0), a(1), a(2),
			},
		},
		{
			assertion:     "many chunks, early disorder only",
			minBufferSize: 1126,
			messages: []msg{
				a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8), a(9), a(0),
				a(10), a(11), a(12), a(13), a(14), a(15), a(16), a(17), a(18), a(19),
				a(20), a(21), a(22), a(23), a(24), a(25), a(26), a(27), a(28), a(29),
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			testfile := makeMcap(t, c.messages)
			rs := bytes.NewReader(testfile)
			info := getInfo(t, rs)
			_, err := NewOrderedLexer(rs, info, c.minBufferSize-1, nil)
			assert.ErrorIs(t, err, ErrWouldExceedMemoryLimit)

			_, err = rs.Seek(0, io.SeekStart)
			assert.Nil(t, err)
			_, err = NewOrderedLexer(rs, info, c.minBufferSize, nil)
			assert.Nil(t, err)
		})
	}
}

func BenchmarkMCAPReaders(b *testing.B) {
	cases := []struct {
		assertion string
		readFunc  func(b *testing.B, rs io.ReadSeeker) uint64
	}{
		{
			assertion: "lexer",
			readFunc: func(b *testing.B, rs io.ReadSeeker) uint64 {
				lexer, err := NewLexer(rs, &LexerOptions{ValidateChunkCRCs: true})
				assert.Nil(b, err)
				defer lexer.Close()
				buf := make([]byte, 4*1024*1024)
				var count uint64
				for {
					tkn, newBuf, err := lexer.Next(buf)
					if tkn == TokenMessage {
						count++
					}
					if len(newBuf) > len(buf) {
						buf = newBuf
					}
					if errors.Is(err, io.EOF) {
						return count
					}
					assert.Nil(b, err)
				}
			},
		}, {
			assertion: "ordered lexer",
			readFunc: func(b *testing.B, rs io.ReadSeeker) uint64 {
				info := getInfo(b, rs)
				ol, err := NewOrderedLexer(rs, info, 64*1024*1024, nil)
				assert.Nil(b, err)
				buf := make([]byte, 4*1024*1024)
				var count uint64
				for {
					tkn, newBuf, err := ol.Next(buf)
					if len(newBuf) >= len(buf) {
						buf = newBuf
					}
					if tkn == TokenMessage {
						count++
					}
					if errors.Is(err, io.EOF) {
						return count
					}
					assert.Nil(b, err)
				}
			},
		}, {
			assertion: "reader",
			readFunc: func(b *testing.B, rs io.ReadSeeker) uint64 {
				reader, err := NewReader(rs)
				assert.Nil(b, err)
				defer reader.Close()
				buf := make([]byte, 4*1024*1024)
				it, err := reader.Messages(InOrder(LogTimeOrder))
				assert.Nil(b, err)
				var count uint64
				for {
					_, _, _, err := it.Next(buf)
					if errors.Is(err, io.EOF) {
						return count
					}
					assert.Nil(b, err)
					count++
				}
			},
		},
	}
	filename := "../../testdata/mcap/demo.mcap"
	stat, err := os.Stat(filename)
	assert.Nil(b, err)
	size := stat.Size()
	var myCount uint64
	for _, c := range cases {
		b.Run(c.assertion, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				handle, err := os.Open(filename)
				assert.Nil(b, err)
				t0 := time.Now()
				count := c.readFunc(b, handle)
				elapsed := time.Since(t0)
				handle.Close()
				if myCount == 0 {
					myCount = count
				}
				assert.Nil(b, err)
				assert.Equal(b, myCount, count)
				b.ReportMetric(float64(size/(1024*1024))/elapsed.Seconds(), "MB/s")
			}
		})
	}
}
