package mcap

import (
	"fmt"
)

type unindexedMessageIterator struct {
	lexer    *Lexer
	schemas  slicemap[Schema]
	channels slicemap[Channel]
	topics   map[string]bool
	start    uint64
	end      uint64

	info *Info

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
			it.schemas.Set(schema.ID, schema)
		case TokenChannel:
			channelInfo, err := ParseChannel(record)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to parse channel info: %w", err)
			}
			if len(it.topics) == 0 || it.topics[channelInfo.Topic] {
				it.channels.Set(channelInfo.ID, channelInfo)
			}
		case TokenMessage:
			if err := msg.PopulateFrom(record, true); err != nil {
				return nil, nil, nil, err
			}
			channel := it.channels.Get(msg.ChannelID)
			if channel == nil {
				// skip messages on channels we don't know about. Note that if
				// an unindexed reader encounters a message it would be
				// interested in, but has not yet encountered the corresponding
				// channel ID, it has no option but to skip.
				continue
			}
			if msg.LogTime >= it.start && msg.LogTime < it.end {
				schema := it.schemas.Get(channel.SchemaID)
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

func (it *unindexedMessageIterator) SetBaseInfo(info *Info) error {
	if info == nil {
		return fmt.Errorf("info is nil")
	}
	it.info = info
	return nil
}
