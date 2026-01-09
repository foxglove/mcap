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

func TestIncludeTopic(t *testing.T) {
	must := func(pats []string) []regexp.Regexp {
		out := make([]regexp.Regexp, len(pats))
		for i, p := range pats {
			out[i] = *regexp.MustCompile(p)
		}
		return out
	}
	cases := []struct {
		name    string
		include []string
		exclude []string
		checks  map[string]bool
	}{
		{
			name:    "no filters include all",
			include: nil,
			exclude: nil,
			checks: map[string]bool{
				"camera_a": true,
				"radar_a":  true,
				"":         true,
			},
		},
		{
			name:    "include only matches",
			include: []string{`^camera_.*$`},
			exclude: nil,
			checks: map[string]bool{
				"camera_a": true,
				"camera_b": true,
				"radar_a":  false,
			},
		},
		{
			name:    "include only multiple patterns OR semantics",
			include: []string{`^camera_a$`, `^radar_.*$`},
			exclude: nil,
			checks: map[string]bool{
				"camera_a": true,
				"camera_b": false,
				"radar_a":  true,
				"radar_b":  true,
			},
		},
		{
			name:    "exclude only excludes matches",
			include: nil,
			exclude: []string{`^camera.*$`},
			checks: map[string]bool{
				"camera_a": false,
				"camera_b": false,
				"radar_a":  true,
			},
		},
		{
			name:    "exclude only multiple patterns",
			include: nil,
			exclude: []string{`^camera_a$`, `^radar_.*$`},
			checks: map[string]bool{
				"camera_a": false,
				"camera_b": true,
				"radar_a":  false,
				"radar_b":  false,
			},
		},
		{
			name:    "both include and exclude (include takes precedence in helper)",
			include: []string{`^camera_.*$`},
			exclude: []string{`^camera_a$`},
			checks: map[string]bool{
				"camera_a": true, // include rules apply when include is present
				"camera_b": true,
				"radar_a":  false,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts := &filterOpts{
				includeTopics: must(c.include),
				excludeTopics: must(c.exclude),
			}
			for topic, expected := range c.checks {
				actual := includeTopic(topic, opts)
				assert.Equal(t, expected, actual, "topic=%q", topic)
			}
		})
	}
}

func TestLastPerChannelOutOfOrderProducesLogTimeOrder(t *testing.T) {
	// Build an MCAP whose file order is out-of-order with respect to log time.
	var srcBuf bytes.Buffer
	writer, err := mcap.NewWriter(&srcBuf, &mcap.WriterOptions{
		Chunked:   true,
		ChunkSize: 10,
	})
	require.NoError(t, err)
	require.NoError(t, writer.WriteHeader(&mcap.Header{}))
	require.NoError(t, writer.WriteSchema(&mcap.Schema{ID: 1}))
	require.NoError(t, writer.WriteChannel(&mcap.Channel{ID: 1, SchemaID: 1, Topic: "camera_a"}))
	require.NoError(t, writer.WriteChannel(&mcap.Channel{ID: 2, SchemaID: 1, Topic: "camera_b"}))
	require.NoError(t, writer.WriteChannel(&mcap.Channel{ID: 3, SchemaID: 0, Topic: "radar"}))
	// Out-of-order writes across channels
	writeMsg := func(ch uint16, ts uint64) {
		require.NoError(t, writer.WriteMessage(&mcap.Message{ChannelID: ch, LogTime: ts}))
	}
	writeMsg(1, 25) // camera_a
	writeMsg(2, 30) // camera_b
	writeMsg(3, 21) // radar
	writeMsg(1, 5)  // camera_a
	writeMsg(2, 7)  // camera_b
	writeMsg(3, 6)  // radar
	writeMsg(1, 18) // camera_a
	writeMsg(2, 19) // camera_b
	writeMsg(3, 40) // radar
	writeMsg(3, 17) // radar
	require.NoError(t, writer.Close())

	// Set start=20 and include last-per-channel for camera topics only.
	opts, err := buildFilterOptions(&filterFlags{
		startNano:                   20,
		includeLastPerChannelTopics: []string{"camera_.*"},
	})
	require.NoError(t, err)

	var outBuf bytes.Buffer
	// Seekable input required for last-per-channel behavior.
	require.NoError(t, filter(bytes.NewReader(srcBuf.Bytes()), &outBuf, opts))

	// Parse output and verify:
	// - Entire message stream is in ascending log time order
	// - Exactly two pre-start messages (one per camera_* topic) with times 18 and 19
	// - Remaining messages are >= 20 and in ascending order
	channelByID := map[uint16]string{}
	type m struct {
		topic string
		ts    uint64
	}
	var msgs []m
	lexer, err := mcap.NewLexer(&outBuf, &mcap.LexerOptions{})
	require.NoError(t, err)
	defer lexer.Close()
	for {
		token, record, err := lexer.Next(nil)
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}
		switch token {
		case mcap.TokenChannel:
			ch, err := mcap.ParseChannel(record)
			require.NoError(t, err)
			channelByID[ch.ID] = ch.Topic
		case mcap.TokenMessage:
			msg, err := mcap.ParseMessage(record)
			require.NoError(t, err)
			msgs = append(msgs, m{topic: channelByID[msg.ChannelID], ts: msg.LogTime})
		}
	}
	require.GreaterOrEqual(t, len(msgs), 2)
	// Check global ascending log time order
	var prev uint64
	for i, mm := range msgs {
		if i > 0 {
			assert.LessOrEqual(t, prev, mm.ts, "messages must be in non-decreasing log time order")
		}
		prev = mm.ts
	}
	// Identify pre-start messages
	pre := []m{}
	for _, mm := range msgs {
		if mm.ts < 20 {
			pre = append(pre, mm)
		}
	}
	require.Len(t, pre, 2, "expected exactly two pre-start messages (one per camera topic)")
	// Expect the specific pre-start times and topics
	assert.ElementsMatch(t,
		[]m{{topic: "camera_a", ts: 18}, {topic: "camera_b", ts: 19}},
		pre,
	)
	// Post-start should be all >= 20 and include expected times in ascending order
	post := []uint64{}
	for _, mm := range msgs {
		if mm.ts >= 20 {
			post = append(post, mm.ts)
		}
	}
	require.NotEmpty(t, post)
	for _, ts := range post {
		require.GreaterOrEqual(t, ts, uint64(20))
	}
	// Expected >=20 times from input: 21(radar), 25(camera_a), 30(camera_b), 40(radar)
	assert.Equal(t, []uint64{21, 25, 30, 40}, post)
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
		{name: "does not override exclude topics",
			flags: &filterFlags{
				startNano:                   50,
				includeLastPerChannelTopics: []string{"camera_.*"},
				excludeTopics:               []string{"camera_a"},
			},
			expectedMessageCount: map[uint16]int{
				1: 0,
				2: 51,
				3: 50,
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
