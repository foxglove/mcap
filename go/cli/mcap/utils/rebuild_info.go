package utils

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
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
					if err != nil {
						return nil, err
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
		return messageIndexes, nil
	}

	return nil, nil
}

type RebuildData struct {
	// Contains everything needed to write a new summary section
	Info *mcap.Info
	// Offset of DataEnd, writing at this offset a DataEnd record to recover it.
	DataEndOffset int64
	// ContainsFaultyChunks indicates if the file contains chunks that are not
	ContainsFaultyChunks bool
	// MessageIndexes for the lust chunk, if any.
	MessageIndexes []*mcap.MessageIndex

	DataSectionCRC uint32
}

// RebuildInfo reads an MCAP file and rebuilds the info from it.
func RebuildInfo(r io.ReadSeeker) (*RebuildData, error) {
	info := &mcap.Info{
		Statistics: &mcap.Statistics{
			ChannelMessageCounts: make(map[uint16]uint64),
		},
		Channels:          make(map[uint16]*mcap.Channel),
		Schemas:           make(map[uint16]*mcap.Schema),
		ChunkIndexes:      make([]*mcap.ChunkIndex, 0),
		MetadataIndexes:   make([]*mcap.MetadataIndex, 0),
		AttachmentIndexes: make([]*mcap.AttachmentIndex, 0),
		Header: &mcap.Header{
			Profile: "",
			Library: "",
		},
		Footer: &mcap.Footer{},
	}

	rebuildData := &RebuildData{
		Info: info,
	}

	var position int64

	lexer, err := mcap.NewLexer(r, &mcap.LexerOptions{
		ValidateChunkCRCs: true,
		EmitChunks:        true,
		EmitInvalidChunks: true,
		AttachmentCallback: func(ar *mcap.AttachmentReader) error {
			info.MetadataIndexes = append(info.MetadataIndexes, &mcap.MetadataIndex{
				Offset: uint64(position),
				Length: ar.DataSize,
				Name:   ar.Name,
			})
			return nil
		},
	})
	if err != nil {
		return nil, err
	}

	var lastChunk *mcap.Chunk
	var lastIndexes []*mcap.MessageIndex
	var messageIndexOffsets map[uint16]uint64
	var chunkStartOffset int64
	var chunkEndOffset int64

	finalizeChunk := func() {
		if lastChunk != nil {
			messageIndexEnd := uint64(position)
			info.ChunkIndexes = append(info.ChunkIndexes, &mcap.ChunkIndex{
				MessageStartTime:    lastChunk.MessageStartTime,
				MessageEndTime:      lastChunk.MessageEndTime,
				ChunkStartOffset:    uint64(chunkStartOffset),
				ChunkLength:         uint64(chunkEndOffset - chunkStartOffset),
				MessageIndexOffsets: messageIndexOffsets,
				MessageIndexLength:  messageIndexEnd - uint64(chunkEndOffset),
				Compression:         mcap.CompressionFormat(lastChunk.Compression),
				CompressedSize:      uint64(len(lastChunk.Records)),
				UncompressedSize:    lastChunk.UncompressedSize,
			})

			lastIndexes = nil
			lastChunk = nil
			messageIndexOffsets = nil
		}
	}
	buf := make([]byte, 1024)
	bufLen := 0
	doneReading := false
	for !doneReading {
		// The offset of the previous read it the last valid position
		rebuildData.DataEndOffset = position

		position, err = r.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		rebuildData.DataSectionCRC = crc32.Update(rebuildData.DataSectionCRC, crc32.IEEETable, buf[:bufLen])

		token, data, err := lexer.Next(buf)
		bufLen = len(data)
		if err != nil {
			if token == mcap.TokenInvalidChunk {
				fmt.Printf("Invalid chunk encountered, skipping: %s\n", err)
				continue
			}
			if errors.Is(err, io.EOF) {
				break
			}
			var expected *mcap.ErrTruncatedRecord
			if errors.As(err, &expected) {
				fmt.Println(expected.Error())
				break
			}
			break
		}
		if len(data) > len(buf) {
			buf = data
		}

		if token != mcap.TokenMessageIndex {
			if lastChunk != nil {
				idx, err := UpdateInfoFromChunk(info, lastChunk, lastIndexes)
				if err != nil {
					fmt.Printf("Failed to read chunk, skipping: %s\n", err)
					rebuildData.ContainsFaultyChunks = true
					lastChunk = nil
				}
				if idx != nil {
					fmt.Println("Unexpected message index")
				}
				finalizeChunk()
			}
		}

		switch token {
		case mcap.TokenHeader:
			header, err := mcap.ParseHeader(data)
			if err != nil {
				return nil, err
			}

			info.Header = header

		case mcap.TokenChunk:
			chunk, err := mcap.ParseChunk(data)
			if err != nil {
				return nil, err
			}

			if info.Statistics.MessageCount == 0 {
				info.Statistics.MessageStartTime = chunk.MessageStartTime
			}
			if info.Statistics.MessageEndTime < chunk.MessageEndTime {
				info.Statistics.MessageEndTime = chunk.MessageEndTime
			}

			// copy the records, since it is referenced and the buffer will be reused
			recordsCopy := make([]byte, len(chunk.Records))
			copy(recordsCopy, chunk.Records)
			lastChunk = chunk
			lastChunk.Records = recordsCopy
			messageIndexOffsets = make(map[uint16]uint64)
			chunkStartOffset = position
			chunkEndOffset, err = r.Seek(0, io.SeekCurrent)
			if err != nil {
				return nil, err
			}
		case mcap.TokenMessageIndex:
			if lastChunk == nil {
				return nil, fmt.Errorf("got message index but not chunk before it")
			}
			index, err := mcap.ParseMessageIndex(data)
			if err != nil {
				return nil, err
			}
			lastIndexes = append(lastIndexes, index)
			messageIndexOffsets[index.ChannelID] = uint64(position)
		case mcap.TokenMetadata:
			metadata, err := mcap.ParseMetadata(data)
			if err != nil {
				return nil, err
			}
			info.MetadataIndexes = append(info.MetadataIndexes, &mcap.MetadataIndex{
				Offset: uint64(position),
				Length: uint64(len(data)),
				Name:   metadata.Name,
			})
		case mcap.TokenDataEnd, mcap.TokenFooter:
			// data section is over, either because the file is over or the summary section starts.
			doneReading = true
			// Set the end offset for the data section
			rebuildData.DataEndOffset = position
		case mcap.TokenSchema, mcap.TokenChannel, mcap.TokenMessage:
			return nil, fmt.Errorf("rebuilding info only supports chunked mcaps")
		case mcap.TokenError:
			return nil, errors.New("received error token but lexer did not return error on Next")
		}
	}

	if lastChunk != nil {
		idx, err := UpdateInfoFromChunk(info, lastChunk, nil)
		if err != nil {
			fmt.Printf("Failed to read chunk, skipping: %s\n", err)
			rebuildData.ContainsFaultyChunks = true
			lastChunk = nil
		}
		rebuildData.MessageIndexes = idx
	}

	info.Statistics.ChannelCount = uint32(len(info.Channels))
	info.Statistics.SchemaCount = uint16(len(info.Schemas))
	info.Statistics.ChunkCount = uint32(len(info.ChunkIndexes))
	info.Statistics.AttachmentCount = uint32(len(info.AttachmentIndexes))
	info.Statistics.MetadataCount = uint32(len(info.MetadataIndexes))

	return rebuildData, nil
}

// WriteInfo writes the summary section to the given writer using the provided info.
// Ensure that the cursor is just behind the DataEnd record.
func WriteInfo(w io.WriteSeeker, info *mcap.Info) error {
	position, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	section := &summarySection{
		Channels:          make([]*mcap.Channel, 0),
		Schemas:           make([]*mcap.Schema, 0),
		AttachmentIndexes: info.AttachmentIndexes,
		MetadataIndexes:   info.MetadataIndexes,
		ChunkIndexes:      info.ChunkIndexes,

		Statistics: info.Statistics,
		Footer: &mcap.Footer{
			SummaryCRC: 1, // Dummy value, so `writeSummaryBytes` write it
		},
	}

	for _, channel := range info.Channels {
		section.Channels = append(section.Channels, channel)
	}
	for _, schema := range info.Schemas {
		section.Schemas = append(section.Schemas, schema)
	}

	return writeSummaryBytes(w, section, position)
}
