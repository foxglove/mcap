package mcap

import (
	"fmt"

	"github.com/foxglove/mcap/go/mcap/slicemap"
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
	return it.Next2(msg)
}

func (it *unindexedMessageIterator) Next2(msg *Message) (*Schema, *Channel, *Message, error) {
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
			it.schemas = slicemap.SetAt(it.schemas, schema.ID, schema)
		case TokenChannel:
			channelInfo, err := ParseChannel(record)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to parse channel info: %w", err)
			}
			if len(it.topics) == 0 || it.topics[channelInfo.Topic] {
				it.channels = slicemap.SetAt(it.channels, channelInfo.ID, channelInfo)
			}
		case TokenMessage:
			if err := msg.PopulateFrom(record); err != nil {
				return nil, nil, nil, err
			}
			channel := slicemap.GetAt(it.channels, msg.ChannelID)
			if channel == nil {
				// skip messages on channels we don't know about. Note that if
				// an unindexed reader encounters a message it would be
				// interested in, but has not yet encountered the corresponding
				// channel ID, it has no option but to skip.
				continue
			}
			if msg.LogTime >= it.start && msg.LogTime < it.end {
				schema := slicemap.GetAt(it.schemas, channel.SchemaID)
				if schema == nil && channel.SchemaID != 0 {
					return nil, nil, nil, fmt.Errorf("channel %d with unrecognized schema ID %d", msg.ChannelID, channel.SchemaID)
				}
				return schema, channel, msg, nil
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
