package utils

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// UpdateInfoFromChunk updates the info object with the information from the chunk.
// If chunk contains new unseen channels, add them to the info.
// If messageIndex is nil, it will be generated from the chunk and returned.
func UpdateInfoFromChunk(
	info *mcap.Info, c *mcap.Chunk, messageIndexes []*mcap.MessageIndex,
) ([]*mcap.MessageIndex, error) {
	containsNewChannel := false
	recreateMessageIndexes := false
	var messageIndexesByChannelID map[uint16]*mcap.MessageIndex

	if messageIndexes == nil {
		recreateMessageIndexes = true
		messageIndexesByChannelID = make(map[uint16]*mcap.MessageIndex)
	} else {
		for _, messageIndex := range messageIndexes {
			if messageIndex.IsEmpty() {
				continue
			}
			if _, ok := info.Channels[messageIndex.ChannelID]; !ok {
				containsNewChannel = true
			}
			info.Statistics.MessageCount += uint64(len(messageIndex.Records))
			info.Statistics.ChannelMessageCounts[messageIndex.ChannelID] += uint64(len(messageIndex.Records))
		}
	}

	if containsNewChannel || recreateMessageIndexes {
		var uncompressedBytes []byte

		switch mcap.CompressionFormat(c.Compression) {
		case mcap.CompressionNone:
			uncompressedBytes = c.Records
		case mcap.CompressionZSTD:
			compressedDataReader := bytes.NewReader(c.Records)
			chunkDataReader, err := zstd.NewReader(compressedDataReader)
			if err != nil {
				return nil, err
			}
			defer chunkDataReader.Close()
			uncompressedBytes, err = io.ReadAll(chunkDataReader)
			if err != nil {
				return nil, err
			}
		case mcap.CompressionLZ4:
			var err error
			compressedDataReader := bytes.NewReader(c.Records)
			chunkDataReader := lz4.NewReader(compressedDataReader)
			uncompressedBytes, err = io.ReadAll(chunkDataReader)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported compression format: %s", c.Compression)
		}

		uncompressedBytesReader := bytes.NewReader(uncompressedBytes)

		lexer, err := mcap.NewLexer(uncompressedBytesReader, &mcap.LexerOptions{
			SkipMagic: true,
		})
		if err != nil {
			return nil, err
		}
		defer lexer.Close()

		msg := make([]byte, 1024)
		for {
			position, err := uncompressedBytesReader.Seek(0, io.SeekCurrent)
			if err != nil {
				return nil, err
			}
			token, data, err := lexer.Next(msg)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, err
			}
			if len(data) > len(msg) {
				msg = data
			}

			switch token {
			case mcap.TokenSchema:
				schema, err := mcap.ParseSchema(data)
				if err != nil {
					return nil, err
				}
				info.Schemas[schema.ID] = schema
			case mcap.TokenChannel:
				channel, err := mcap.ParseChannel(data)
				if err != nil {
					return nil, err
				}
				info.Channels[channel.ID] = channel
			case mcap.TokenMessage:
				if recreateMessageIndexes {
					m, err := mcap.ParseMessage(data)
					if err != nil {
						return nil, err
					}
					idx, ok := messageIndexesByChannelID[m.ChannelID]
					if !ok {
						idx = &mcap.MessageIndex{
							ChannelID: m.ChannelID,
							Records:   nil,
						}
						messageIndexesByChannelID[m.ChannelID] = idx
					}
					idx.Add(m.LogTime, uint64(position))

					// Also update stats if recreating indexes
					info.Statistics.MessageCount++
					info.Statistics.ChannelMessageCounts[m.ChannelID]++
				}
			}
		}
	}

	if recreateMessageIndexes {
		messageIndexes = make([]*mcap.MessageIndex, 0, len(messageIndexesByChannelID))
		for _, idx := range messageIndexesByChannelID {
			messageIndexes = append(messageIndexes, idx)
		}
	}

	return messageIndexes, nil
}
