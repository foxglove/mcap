package cmd

import (
	"bytes"
	"io"
	"regexp"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
)

func writeFilterTestInput(t *testing.T, w io.Writer) {
	writer, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Chunked: true,
	})
	assert.Nil(t, err)

	assert.Nil(t, writer.WriteHeader(&mcap.Header{}))
	assert.Nil(t, writer.WriteSchema(&mcap.Schema{
		ID: 1,
	}))
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:       1,
		SchemaID: 1,
		Topic:    "camera_a",
	}))
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:       2,
		SchemaID: 1,
		Topic:    "camera_b",
	}))
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:       3,
		SchemaID: 1,
		Topic:    "radar_a",
	}))
	for i := 0; i < 100; i++ {
		assert.Nil(t, writer.WriteMessage(&mcap.Message{
			ChannelID: 1,
			LogTime:   uint64(i),
		}))
		assert.Nil(t, writer.WriteMessage(&mcap.Message{
			ChannelID: 2,
			LogTime:   uint64(i),
		}))
		assert.Nil(t, writer.WriteMessage(&mcap.Message{
			ChannelID: 3,
			LogTime:   uint64(i),
		}))
	}
	assert.Nil(t, writer.WriteAttachment(&mcap.Attachment{
		LogTime: 50,
		Name:    "attachment",
	}))
	assert.Nil(t, writer.WriteMetadata(&mcap.Metadata{
		Name: "metadata",
	}))
	assert.Nil(t, writer.Close())
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
			writeBuf := bytes.Buffer{}
			readBuf := bytes.Buffer{}

			writeFilterTestInput(t, &readBuf)
			assert.Nil(t, filter(&readBuf, &writeBuf, c.opts))

			lexer, err := mcap.NewLexer(&writeBuf)
			assert.Nil(t, err)
			messageCounter := map[uint16]int{
				1: 0,
				2: 0,
				3: 0,
			}
			attachmentCounter := 0
			metadataCounter := 0
			for {
				token, record, err := lexer.Next(nil)
				if err != nil {
					assert.ErrorIs(t, err, io.EOF)
					break
				}
				switch token {
				case mcap.TokenMessage:
					message, err := mcap.ParseMessage(record)
					assert.Nil(t, err)
					messageCounter[message.ChannelID]++
				case mcap.TokenAttachment:
					attachmentCounter++
				case mcap.TokenMetadata:
					metadataCounter++
				}
			}
			assert.Equal(t, c.expectedAttachmentCount, attachmentCounter)
			assert.Equal(t, c.expectedMetadataCount, metadataCounter)
			assert.InDeltaMapValues(t, c.expectedMessageCount, messageCounter, 0.0)
		})
	}
}
