package utils

import (
	"bytes"
	"io"
	"testing"

	"github.com/foxglove/mcap/go/cli/mcap/testutils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
)

func TestAmendsIndexedFile(t *testing.T) {
	buf := testutils.NewBufReadWriteSeeker()
	writer, err := mcap.NewWriter(buf, &mcap.WriterOptions{
		IncludeCRC: true,
		Chunked:    true,
		ChunkSize:  1024,
	})
	assert.Nil(t, err)
	assert.Nil(t, writer.WriteHeader(&mcap.Header{}))
	assert.Nil(t, writer.WriteSchema(&mcap.Schema{
		ID:       1,
		Name:     "s1",
		Encoding: "txt",
		Data:     []byte{0x01, 0x02, 0x03},
	}))
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:              0,
		SchemaID:        1,
		Topic:           "/topic",
		MessageEncoding: "txt",
		Metadata: map[string]string{
			"happy": "days",
		},
	}))
	for i := 0; i < 100; i++ {
		assert.Nil(t, writer.WriteMessage(&mcap.Message{
			ChannelID: 0,
			Data:      []byte{0x01, 0x02, 0x03},
		}))
	}
	assert.Nil(t, writer.Close())

	_, err = buf.Seek(0, io.SeekStart)
	assert.Nil(t, err)

	reader, err := mcap.NewReader(buf)
	assert.Nil(t, err)
	initialInfo, err := reader.Info()
	assert.Nil(t, err)
	reader.Close()

	assert.Nil(t, AmendMCAP(buf, []*mcap.Attachment{
		{
			LogTime:    0,
			CreateTime: 0,
			Name:       "a1",
			MediaType:  "text/plain",
			DataSize:   10,
			Data:       bytes.NewReader(make([]byte, 10)),
		},
	}, nil))

	_, err = buf.Seek(0, io.SeekStart)
	assert.Nil(t, err)
	reader, err = mcap.NewReader(buf)
	assert.Nil(t, err)
	newInfo, err := reader.Info()
	assert.Nil(t, err)
	assert.Equal(t, 1, int(newInfo.Statistics.AttachmentCount))
	assert.Equal(t, initialInfo.Statistics.MessageCount, newInfo.Statistics.MessageCount)
	assert.Equal(t, initialInfo.Statistics.ChannelCount, newInfo.Statistics.ChannelCount)
	assert.Equal(t, initialInfo.Statistics.MetadataCount, newInfo.Statistics.MetadataCount)
	assert.Equal(t, initialInfo.Channels, newInfo.Channels)
	assert.Equal(t, initialInfo.Schemas, newInfo.Schemas)
	assert.Positive(t, newInfo.Footer.SummaryCRC)
}

func TestDoesNotComputeCRCIfDisabled(t *testing.T) {
	buf := testutils.NewBufReadWriteSeeker()
	writer, err := mcap.NewWriter(buf, &mcap.WriterOptions{
		IncludeCRC: false,
		Chunked:    true,
		ChunkSize:  1024,
	})
	assert.Nil(t, err)
	assert.Nil(t, writer.WriteHeader(&mcap.Header{}))
	assert.Nil(t, writer.WriteSchema(&mcap.Schema{
		ID:       1,
		Name:     "s1",
		Encoding: "txt",
		Data:     []byte{0x01, 0x02, 0x03},
	}))
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:              0,
		SchemaID:        1,
		Topic:           "/topic",
		MessageEncoding: "txt",
		Metadata: map[string]string{
			"happy": "days",
		},
	}))
	for i := 0; i < 100; i++ {
		assert.Nil(t, writer.WriteMessage(&mcap.Message{
			ChannelID: 0,
			Data:      []byte{0x01, 0x02, 0x03},
		}))
	}
	assert.Nil(t, writer.Close())

	_, err = buf.Seek(0, io.SeekStart)
	assert.Nil(t, err)

	reader, err := mcap.NewReader(buf)
	assert.Nil(t, err)
	initialInfo, err := reader.Info()
	assert.Nil(t, err)
	reader.Close()

	assert.Nil(t, AmendMCAP(buf, []*mcap.Attachment{
		{
			LogTime:    0,
			CreateTime: 0,
			Name:       "a1",
			MediaType:  "text/plain",
			DataSize:   10,
			Data:       bytes.NewReader(make([]byte, 10)),
		},
	}, nil))

	_, err = buf.Seek(0, io.SeekStart)
	assert.Nil(t, err)
	reader, err = mcap.NewReader(buf)
	assert.Nil(t, err)
	newInfo, err := reader.Info()
	assert.Nil(t, err)
	assert.Equal(t, 1, int(newInfo.Statistics.AttachmentCount))
	assert.Equal(t, initialInfo.Statistics.MessageCount, newInfo.Statistics.MessageCount)
	assert.Equal(t, initialInfo.Statistics.ChannelCount, newInfo.Statistics.ChannelCount)
	assert.Equal(t, initialInfo.Statistics.MetadataCount, newInfo.Statistics.MetadataCount)
	assert.Equal(t, initialInfo.Channels, newInfo.Channels)
	assert.Equal(t, initialInfo.Schemas, newInfo.Schemas)
	assert.Zero(t, newInfo.Footer.SummaryCRC)
}

func TestAmendsUnindexedFile(t *testing.T) {
	buf := testutils.NewBufReadWriteSeeker()
	writer, err := mcap.NewWriter(buf, &mcap.WriterOptions{
		IncludeCRC: false,
		Chunked:    false,
		ChunkSize:  1024,
	})
	assert.Nil(t, err)
	assert.Nil(t, writer.WriteHeader(&mcap.Header{}))
	assert.Nil(t, writer.WriteSchema(&mcap.Schema{
		ID:       1,
		Name:     "s1",
		Encoding: "txt",
		Data:     []byte{0x01, 0x02, 0x03},
	}))
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:              0,
		SchemaID:        1,
		Topic:           "/topic",
		MessageEncoding: "txt",
		Metadata: map[string]string{
			"happy": "days",
		},
	}))
	for i := 0; i < 100; i++ {
		assert.Nil(t, writer.WriteMessage(&mcap.Message{
			ChannelID: 0,
			Data:      []byte{0x01, 0x02, 0x03},
		}))
	}
	assert.Nil(t, writer.Close())

	_, err = buf.Seek(0, io.SeekStart)
	assert.Nil(t, err)

	reader, err := mcap.NewReader(buf)
	assert.Nil(t, err)
	initialInfo, err := reader.Info()
	assert.Nil(t, err)
	reader.Close()

	assert.Nil(t, AmendMCAP(buf, []*mcap.Attachment{
		{
			LogTime:    0,
			CreateTime: 0,
			Name:       "a1",
			MediaType:  "text/plain",
			DataSize:   10,
			Data:       bytes.NewReader(make([]byte, 10)),
		},
	}, nil))

	_, err = buf.Seek(0, io.SeekStart)
	assert.Nil(t, err)
	reader, err = mcap.NewReader(buf)
	assert.Nil(t, err)
	newInfo, err := reader.Info()
	assert.Nil(t, err)
	assert.Equal(t, 1, int(newInfo.Statistics.AttachmentCount))
	assert.Equal(t, 100, int(newInfo.Statistics.MessageCount))
	assert.Equal(t, 1, int(newInfo.Statistics.ChannelCount))
	assert.Equal(t, 0, int(newInfo.Statistics.MetadataCount))
	assert.Equal(t, initialInfo.Channels, newInfo.Channels)
	assert.Equal(t, initialInfo.Schemas, newInfo.Schemas)
	assert.Zero(t, newInfo.Footer.SummaryCRC)
}
