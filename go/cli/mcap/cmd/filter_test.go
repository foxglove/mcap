package cmd

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFilterTestInput(t *testing.T, w io.Writer) {
	mcap.Version = "1"
	writer, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Chunked:   true,
		ChunkSize: 10,
	})
	require.NoError(t, err)

	require.NoError(t, writer.WriteHeader(&mcap.Header{}))
	require.NoError(t, writer.WriteSchema(&mcap.Schema{
		ID: 1,
	}))
	require.NoError(t, writer.WriteChannel(&mcap.Channel{
		ID:       1,
		SchemaID: 1,
		Topic:    "camera_a",
	}))
	require.NoError(t, writer.WriteChannel(&mcap.Channel{
		ID:       2,
		SchemaID: 1,
		Topic:    "camera_b",
	}))
	require.NoError(t, writer.WriteChannel(&mcap.Channel{
		ID:       3,
		SchemaID: 0,
		Topic:    "radar_a",
	}))
	for i := 0; i < 100; i++ {
		require.NoError(t, writer.WriteMessage(&mcap.Message{
			ChannelID: 1,
			LogTime:   uint64(i),
		}))
		require.NoError(t, writer.WriteMessage(&mcap.Message{
			ChannelID: 2,
			LogTime:   uint64(i),
		}))
		require.NoError(t, writer.WriteMessage(&mcap.Message{
			ChannelID: 3,
			LogTime:   uint64(i),
		}))
	}
	require.NoError(t, writer.WriteAttachment(&mcap.Attachment{
		LogTime: 50,
		Name:    "attachment",
		Data:    bytes.NewReader(nil),
	}))
	require.NoError(t, writer.WriteMetadata(&mcap.Metadata{
		Name: "metadata",
	}))
	require.NoError(t, writer.Close())
}
func TestPassthrough(t *testing.T) {
	opts := &filterOpts{
		compressionFormat:  mcap.CompressionLZ4,
		start:              0,
		end:                1000,
		includeAttachments: true,
		includeMetadata:    true,
	}

	for _, seekable := range []bool{false, true} {
		t.Run(fmt.Sprintf("seekable=%v", seekable), func(t *testing.T) {
			writeBuf := bytes.Buffer{}
			readBuf := bytes.Buffer{}

			writeFilterTestInput(t, &readBuf)
			var src io.Reader = &readBuf
			if seekable {
				src = bytes.NewReader(readBuf.Bytes())
			}
			require.NoError(t, filter(src, &writeBuf, opts))
			attachmentCounter := 0
			metadataCounter := 0
			schemaCounter := 0
			messageCounter := map[uint16]int{
				1: 0,
				2: 0,
				3: 0,
			}
			channelCounter := map[uint16]int{
				1: 0,
				2: 0,
				3: 0,
			}
			lexer, err := mcap.NewLexer(&writeBuf, &mcap.LexerOptions{
				AttachmentCallback: func(*mcap.AttachmentReader) error {
					attachmentCounter++
					return nil
				},
			})
			require.NoError(t, err)
			defer lexer.Close()
			for {
				token, record, err := lexer.Next(nil)
				if err != nil {
					require.ErrorIs(t, err, io.EOF)
					break
				}
				switch token {
				case mcap.TokenMessage:
					message, err := mcap.ParseMessage(record)
					require.NoError(t, err)
					messageCounter[message.ChannelID]++
				case mcap.TokenChannel:
					channel, err := mcap.ParseChannel(record)
					require.NoError(t, err)
					channelCounter[channel.ID]++
				case mcap.TokenSchema:
					schemaCounter++
				case mcap.TokenMetadata:
					metadataCounter++
				}
			}
			assert.Equal(t, 1, attachmentCounter)
			assert.Equal(t, 1, metadataCounter)
			assert.InDeltaMapValues(t, map[uint16]int{1: 100, 2: 100, 3: 100}, messageCounter, 0.0)
			// schemas and channels should be duplicated once into the summary section
			assert.Equal(t, 2, schemaCounter)
			assert.InDeltaMapValues(t, map[uint16]int{1: 2, 2: 2, 3: 2}, channelCounter, 0.0)
		})
	}
}

func TestFiltering(t *testing.T) {
	cases := []struct {
		name                    string
		opts                    *filterOpts
		expectedMessageCount    map[uint16]int
		expectedAttachmentCount int
		expectedMetadataCount   int
	}{
		{
			name: "inclusive topic filtering",
			opts: &filterOpts{
				compressionFormat: mcap.CompressionLZ4,
				start:             0,
				end:               1000,
				includeTopics:     []regexp.Regexp{*regexp.MustCompile("camera.*")},
			},
			expectedMessageCount: map[uint16]int{
				1: 100,
				2: 100,
				3: 0,
			},
		},
		{
			name: "double exclusive topic filtering",
			opts: &filterOpts{
				compressionFormat: mcap.CompressionLZ4,
				start:             0,
				end:               1000,
				excludeTopics: []regexp.Regexp{
					*regexp.MustCompile("camera_a"),
					*regexp.MustCompile("camera_b"),
				},
			},
			expectedMessageCount: map[uint16]int{
				1: 0,
				2: 0,
				3: 100,
			},
		},
		{
			name: "exclusive filtering and including attachments",
			opts: &filterOpts{
				compressionFormat:  mcap.CompressionLZ4,
				start:              0,
				end:                1000,
				excludeTopics:      []regexp.Regexp{*regexp.MustCompile("camera.*")},
				includeAttachments: true,
			},
			expectedMessageCount: map[uint16]int{
				1: 0,
				2: 0,
				3: 100,
			},
			expectedAttachmentCount: 1,
		},
		{
			name: "time range filtering (including attachments)",
			opts: &filterOpts{
				compressionFormat:  mcap.CompressionLZ4,
				start:              0,
				end:                49,
				includeAttachments: true,
			},
			expectedMessageCount: map[uint16]int{
				1: 49,
				2: 49,
				3: 49,
			},
			expectedAttachmentCount: 0,
		},
		{
			name: "including metadata",
			opts: &filterOpts{
				compressionFormat: mcap.CompressionLZ4,
				start:             0,
				end:               1000,
				includeMetadata:   true,
			},
			expectedMessageCount: map[uint16]int{
				1: 100,
				2: 100,
				3: 100,
			},
			expectedAttachmentCount: 0,
			expectedMetadataCount:   1,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, seekable := range []bool{false, true} {
				t.Run(fmt.Sprintf("seekable=%v", seekable), func(t *testing.T) {
					writeBuf := bytes.Buffer{}
					readBuf := bytes.Buffer{}

					writeFilterTestInput(t, &readBuf)
					var src io.Reader = &readBuf
					if seekable {
						src = bytes.NewReader(readBuf.Bytes())
					}
					require.NoError(t, filter(src, &writeBuf, c.opts))
					attachmentCounter := 0
					metadataCounter := 0
					lexer, err := mcap.NewLexer(&writeBuf, &mcap.LexerOptions{
						AttachmentCallback: func(*mcap.AttachmentReader) error {
							attachmentCounter++
							return nil
						},
					})
					require.NoError(t, err)
					defer lexer.Close()
					messageCounter := map[uint16]int{
						1: 0,
						2: 0,
						3: 0,
					}
					for {
						token, record, err := lexer.Next(nil)
						if err != nil {
							require.ErrorIs(t, err, io.EOF)
							break
						}
						switch token {
						case mcap.TokenMessage:
							message, err := mcap.ParseMessage(record)
							require.NoError(t, err)
							messageCounter[message.ChannelID]++
						case mcap.TokenMetadata:
							metadataCounter++
						}
					}
					assert.Equal(t, c.expectedAttachmentCount, attachmentCounter)
					assert.Equal(t, c.expectedMetadataCount, metadataCounter)
					assert.InDeltaMapValues(t, c.expectedMessageCount, messageCounter, 0.0)
				})
			}
		})
	}
}

func TestCompileMatchers(t *testing.T) {
	matchers, err := compileMatchers([]string{"camera.*", "lights.*"})
	require.NoError(t, err)
	assert.Len(t, matchers, 2)
	assert.True(t, matchers[0].MatchString("camera"))
	assert.True(t, matchers[1].MatchString("lights"))
}

func TestParseDateOrNanos(t *testing.T) {
	expected := uint64(1690298850132545471)
	zulu, err := parseDateOrNanos("2023-07-25T15:27:30.132545471Z")
	require.NoError(t, err)
	assert.Equal(t, expected, zulu)
	withTimezone, err := parseDateOrNanos("2023-07-26T01:27:30.132545471+10:00")
	require.NoError(t, err)
	assert.Equal(t, expected, withTimezone)
}

func TestBuildFilterOptions(t *testing.T) {
	cases := []struct {
		name  string
		flags *filterFlags
		opts  *filterOpts
	}{
		{
			name:  "start and end by seconds",
			flags: &filterFlags{startSec: 100, endSec: 1000},
			opts:  &filterOpts{start: 100_000_000_000, end: 1_000_000_000_000},
		},
		{
			name:  "start and end by nanoseconds",
			flags: &filterFlags{startNano: 100, endNano: 1000},
			opts:  &filterOpts{start: 100, end: 1000},
		},
		{
			name:  "start and end by string nanos",
			flags: &filterFlags{start: "100", end: "1000"},
			opts:  &filterOpts{start: 100, end: 1000},
		},
		{
			name:  "start and end by RFC3339 date",
			flags: &filterFlags{start: "2024-11-13T05:12:20.958Z", end: "2024-11-13T05:12:30Z"},
			opts:  &filterOpts{start: 1731474740958000000, end: 1731474750000000000},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			actual, err := buildFilterOptions(c.flags)
			require.NoError(t, err)
			assert.Equal(t, c.opts.start, actual.start)
			assert.Equal(t, c.opts.end, actual.end)
		})
	}
}

func TestLastPerChannelBehavior(t *testing.T) {
	cases := []struct {
		name                 string
		flags                *filterFlags
		expectedMessageCount map[uint16]int
	}{
		{name: "noop",
			flags: &filterFlags{
				startNano: 50,
			},
			expectedMessageCount: map[uint16]int{
				1: 50,
				2: 50,
				3: 50,
			},
		},
		{name: "last per channel on all topics",
			flags: &filterFlags{
				startNano:                   50,
				includeLastPerChannelTopics: []string{".*"},
			},
			expectedMessageCount: map[uint16]int{
				1: 51,
				2: 51,
				3: 51,
			},
		},
		{name: "last per channel on camera topics only",
			flags: &filterFlags{
				startNano:                   50,
				includeLastPerChannelTopics: []string{"camera_.*"},
			},
			expectedMessageCount: map[uint16]int{
				1: 51,
				2: 51,
				3: 50,
			},
		},
		{name: "does not override include topics",
			flags: &filterFlags{
				startNano:                   50,
				includeLastPerChannelTopics: []string{"camera_.*"},
				includeTopics:               []string{"camera_a"},
			},
			expectedMessageCount: map[uint16]int{
				1: 51,
				2: 0,
				3: 0,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, seekable := range []bool{false, true} {
				t.Run(fmt.Sprintf("seekable=%v", seekable), func(t *testing.T) {
					opts, err := buildFilterOptions(c.flags)
					require.NoError(t, err)
					writeBuf := bytes.Buffer{}
					readBuf := bytes.Buffer{}

					writeFilterTestInput(t, &readBuf)
					var src io.Reader = &readBuf
					if seekable {
						src = bytes.NewReader(readBuf.Bytes())
					}
					err = filter(src, &writeBuf, opts)
					// When streaming (non-seekable) and last-per-channel is requested, expect error.
					if !seekable && len(c.flags.includeLastPerChannelTopics) > 0 {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					lexer, err := mcap.NewLexer(&writeBuf, &mcap.LexerOptions{})
					require.NoError(t, err)
					defer lexer.Close()
					messageCounter := map[uint16]int{
						1: 0,
						2: 0,
						3: 0,
					}
					for {
						token, record, err := lexer.Next(nil)
						if err != nil {
							require.ErrorIs(t, err, io.EOF)
							break
						}
						if token == mcap.TokenMessage {
							message, err := mcap.ParseMessage(record)
							require.NoError(t, err)
							messageCounter[message.ChannelID]++
						}
					}
					for channelID, count := range messageCounter {
						require.Equal(
							t,
							c.expectedMessageCount[channelID],
							count,
							"message count incorrect on channel %d", channelID,
						)
					}
				})
			}
		})
	}
}
