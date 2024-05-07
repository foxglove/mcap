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

	metadataCallback func(*Metadata) error
}

func (it *unindexedMessageIterator) Next(p []byte) (*Schema, *Channel, *Message, error) {
	msg := &Message{}
	_, err := it.ReadNextInto(p, msg)
	if err != nil {
		return nil, nil, nil, err
	}
	schema, channel, err := it.SchemaAndChannelForID(msg.ChannelID)
	return schema, channel, msg, err
}

func (it *unindexedMessageIterator) SchemaAndChannelForID(channelID uint16) (*Schema, *Channel, error) {
	channel := it.channels[channelID]
	schema := it.schemas[channel.SchemaID]
	return schema, channel, nil
}

func (it *unindexedMessageIterator) ReadNextInto(buf []byte, msg *Message) ([]byte, error) {
	for {
		tokenType, record, err := it.lexer.Next(buf)
		if err != nil {
			return record, err
		}
		if cap(record) > cap(buf) {
			buf = record
		}
		switch tokenType {
		case TokenSchema:
			schema, err := ParseSchema(record)
			if err != nil {
				return record, fmt.Errorf("failed to parse schema: %w", err)
			}
			if _, ok := it.schemas[schema.ID]; !ok {
				it.schemas[schema.ID] = schema
			}
		case TokenChannel:
			channelInfo, err := ParseChannel(record)
			if err != nil {
				return record, fmt.Errorf("failed to parse channel info: %w", err)
			}
			if _, ok := it.channels[channelInfo.ID]; !ok {
				if len(it.topics) == 0 || it.topics[channelInfo.Topic] {
					it.channels[channelInfo.ID] = channelInfo
				}
			}
		case TokenMessage:
			if err := msg.PopulateFrom(record); err != nil {
				return nil, err
			}
			if _, ok := it.channels[msg.ChannelID]; !ok {
				// skip messages on channels we don't know about. Note that if
				// an unindexed reader encounters a message it would be
				// interested in, but has not yet encountered the corresponding
				// channel ID, it has no option but to skip.
				continue
			}
			if msg.LogTime >= it.start && msg.LogTime < it.end {
				return record, nil
			}
		case TokenMetadata:
			if it.metadataCallback != nil {
				metadata, err := ParseMetadata(record)
				if err != nil {
					return nil, fmt.Errorf("failed to parse metadata: %w", err)
				}
				err = it.metadataCallback(metadata)
				if err != nil {
					return nil, err
				}
			}
			// we don't emit metadata from the reader, so continue onward
			continue
		default:
			// skip all other tokens
		}
	}
}
