package mcap

import (
	"fmt"
)

type unindexedMessageIterator struct {
	lexer    *Lexer
	schemas  map[uint16]*Schema
	channels map[uint16]*Channel
	topics   map[string]bool
	start    uint64
	end      uint64

	recordBuf []byte

	metadataCallback func(*Metadata) error
}

func (it *unindexedMessageIterator) Next(p []byte) (*Schema, *Channel, *Message, error) {
	msg := &Message{Data: p}
	schema, channel, err := it.NextInto(msg)
	if err != nil {
		return nil, nil, nil, err
	}
	return schema, channel, msg, err
}

func (it *unindexedMessageIterator) NextInto(msg *Message) (*Schema, *Channel, error) {
	for {
		tokenType, record, err := it.lexer.Next(it.recordBuf)
		if err != nil {
			return nil, nil, err
		}
		if cap(record) > cap(it.recordBuf) {
			it.recordBuf = record
		}
		switch tokenType {
		case TokenSchema:
			schema, err := ParseSchema(record)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to parse schema: %w", err)
			}
			if _, ok := it.schemas[schema.ID]; !ok {
				it.schemas[schema.ID] = schema
			}
		case TokenChannel:
			channelInfo, err := ParseChannel(record)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to parse channel info: %w", err)
			}
			if _, ok := it.channels[channelInfo.ID]; !ok {
				if len(it.topics) == 0 || it.topics[channelInfo.Topic] {
					it.channels[channelInfo.ID] = channelInfo
				}
			}
		case TokenMessage:
			existingbuf := msg.Data
			if err := msg.PopulateFrom(record); err != nil {
				return nil, nil, err
			}
			msg.Data = append(existingbuf[:0], msg.Data...)
			if _, ok := it.channels[msg.ChannelID]; !ok {
				// skip messages on channels we don't know about. Note that if
				// an unindexed reader encounters a message it would be
				// interested in, but has not yet encountered the corresponding
				// channel ID, it has no option but to skip.
				continue
			}
			if msg.LogTime >= it.start && msg.LogTime < it.end {
				channel, ok := it.channels[msg.ChannelID]
				if !ok {
					return nil, nil, fmt.Errorf("message with unrecognized channel ID %d", msg.ChannelID)
				}
				schema, ok := it.schemas[channel.SchemaID]
				if !ok && channel.SchemaID != 0 {
					return nil, nil, fmt.Errorf("channel %d with unrecognized schema ID %d", msg.ChannelID, channel.SchemaID)
				}
				return schema, channel, nil
			}
		case TokenMetadata:
			if it.metadataCallback != nil {
				metadata, err := ParseMetadata(record)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to parse metadata: %w", err)
				}
				err = it.metadataCallback(metadata)
				if err != nil {
					return nil, nil, err
				}
			}
			// we don't emit metadata from the reader, so continue onward
			continue
		default:
			// skip all other tokens
		}
	}
}
