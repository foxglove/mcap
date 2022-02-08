package libmcap

import (
	"fmt"
)

type unindexedMessageIterator struct {
	lexer    *Lexer
	schemas  map[uint16]*Schema
	channels map[uint16]*ChannelInfo
	topics   map[string]bool
	start    uint64
	end      uint64
}

func (it *unindexedMessageIterator) Next(p []byte) (*ChannelInfo, *Message, error) {
	for {
		tokenType, record, err := it.lexer.Next(p)
		if err != nil {
			return nil, nil, err
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
		case TokenChannelInfo:
			channelInfo, err := ParseChannelInfo(record)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to parse channel info: %w", err)
			}
			if _, ok := it.channels[channelInfo.ID]; !ok {
				if len(it.topics) == 0 || it.topics[channelInfo.Topic] {
					it.channels[channelInfo.ID] = channelInfo
				}
			}
		case TokenMessage:
			message, err := ParseMessage(record)
			if err != nil {
				return nil, nil, err
			}
			if _, ok := it.channels[message.ChannelID]; !ok {
				// skip messages on channels we don't know about. Note that if
				// an unindexed reader encounters a message it would be
				// interested in, but has not yet encountered the corresponding
				// channel ID, it has no option but to skip.
				continue
			}
			if message.LogTime >= it.start && message.LogTime < it.end {
				return it.channels[message.ChannelID], message, nil
			}
		default:
			// skip all other tokens
		}
	}
}
