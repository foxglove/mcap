package mcap

import (
	"fmt"
)

type unindexedMessageIterator struct {
	lexer    *Lexer
	schemas  []*Schema
	channels []*Channel
	topics   map[string]bool
	start    uint64
	end      uint64

	recordBuf []byte

	metadataCallback func(*Metadata) error
}

func (it *unindexedMessageIterator) Next(p []byte) (*Schema, *Channel, *Message, error) {
	msg := &Message{Data: p}
	return it.NextInto(msg)
}

func (it *unindexedMessageIterator) NextInto(msg *Message) (*Schema, *Channel, *Message, error) {
	if msg == nil {
		msg = &Message{}
	}
	for {
		tokenType, record, err := it.lexer.Next(it.recordBuf)
		if err != nil {
			return nil, nil, nil, err
		}
		if cap(record) > cap(it.recordBuf) {
			it.recordBuf = record
		}
		switch tokenType {
		case TokenSchema:
			schema, err := ParseSchema(record)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to parse schema: %w", err)
			}
			if len(it.schemas) <= int(schema.ID) {
				newLen := (int(schema.ID) + 1) * 2
				more := newLen - len(it.schemas)
				it.schemas = append(it.schemas, make([]*Schema, more)...)
			}
			it.schemas[schema.ID] = schema
		case TokenChannel:
			channelInfo, err := ParseChannel(record)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to parse channel info: %w", err)
			}
			if len(it.topics) == 0 || it.topics[channelInfo.Topic] {
				if len(it.channels) <= int(channelInfo.ID) {
					newLen := (int(channelInfo.ID) + 1) * 2
					more := newLen - len(it.channels)
					it.channels = append(it.channels, make([]*Channel, more)...)
				}
				it.channels[channelInfo.ID] = channelInfo
			}
		case TokenMessage:
			existingbuf := msg.Data
			if err := msg.PopulateFrom(record); err != nil {
				return nil, nil, nil, err
			}
			msg.Data = append(existingbuf[:0], msg.Data...)
			if int(msg.ChannelID) >= len(it.channels) || it.channels[msg.ChannelID] == nil {
				// skip messages on channels we don't know about. Note that if
				// an unindexed reader encounters a message it would be
				// interested in, but has not yet encountered the corresponding
				// channel ID, it has no option but to skip.
				continue
			}
			channel := it.channels[msg.ChannelID]
			if msg.LogTime >= it.start && msg.LogTime < it.end {
				if channel.SchemaID == 0 {
					return nil, channel, msg, nil
				}
				if int(channel.SchemaID) >= len(it.schemas) || it.schemas[channel.SchemaID] == nil {
					return nil, nil, nil, fmt.Errorf("channel %d with unrecognized schema ID %d", msg.ChannelID, channel.SchemaID)
				}
				return it.schemas[channel.SchemaID], channel, msg, nil
			}
		case TokenMetadata:
			if it.metadataCallback != nil {
				metadata, err := ParseMetadata(record)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("failed to parse metadata: %w", err)
				}
				err = it.metadataCallback(metadata)
				if err != nil {
					return nil, nil, nil, err
				}
			}
			// we don't emit metadata from the reader, so continue onward
			continue
		default:
			// skip all other tokens
		}
	}
}
