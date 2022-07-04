package mcap

import (
	"fmt"
)

type unindexedContentIterator struct {
	lexer    *Lexer
	schemas  map[uint16]*Schema
	channels map[uint16]*Channel
	config   *contentIteratorConfig
}

func (it *unindexedContentIterator) Next(p []byte) (ContentRecord, error) {
	for {
		tokenType, recordReader, recordLen, err := it.lexer.Next()
		if err != nil {
			return nil, err
		}
		switch tokenType {
		case TokenSchema:
			if it.config.messageFilter == nil {
				continue
			}
			record, err := ReadIntoOrReplace(recordReader, recordLen, &p)
			if err != nil {
				return nil, err
			}
			schema, err := ParseSchema(record)
			if err != nil {
				return nil, fmt.Errorf("failed to parse schema: %w", err)
			}
			if _, ok := it.schemas[schema.ID]; !ok {
				it.schemas[schema.ID] = schema
			}
		case TokenChannel:
			if it.config.messageFilter == nil {
				continue
			}
			record, err := ReadIntoOrReplace(recordReader, recordLen, &p)
			if err != nil {
				return nil, err
			}
			channelInfo, err := ParseChannel(record)
			if err != nil {
				return nil, fmt.Errorf("failed to parse channel info: %w", err)
			}
			it.channels[channelInfo.ID] = channelInfo
		case TokenMessage:
			if it.config.messageFilter == nil {
				continue
			}
			record, err := ReadIntoOrReplace(recordReader, recordLen, &p)
			if err != nil {
				return nil, err
			}
			message, err := ParseMessage(record)
			if err != nil {
				return nil, err
			}
			if it.config.isWithinTimeBounds(message.LogTime) {
				channel := it.channels[message.ChannelID]
				schema := it.schemas[channel.SchemaID]
				if it.config.messageFilter(schema, channel) {
					return &ResolvedMessage{Message: message, Schema: schema, Channel: channel}, nil
				}
			}
		default:
			// skip all other tokens
		}
	}
}
