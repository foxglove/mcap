package libmcap

import (
	"fmt"
	"io"
)

type unindexedMessageIterator struct {
	lexer    *lexer
	channels map[uint16]*ChannelInfo
	topics   map[string]bool
	start    uint64
	end      uint64
}

func (it *unindexedMessageIterator) Next() (*ChannelInfo, *Message, error) {
	for {
		token := it.lexer.Next()
		switch token.TokenType {
		case TokenError:
			return nil, nil, fmt.Errorf("%s", token.bytes())
		case TokenEOF:
			return nil, nil, io.EOF
		case TokenChannelInfo:
			channelInfo, err := parseChannelInfo(token.bytes())
			if err != nil {
				return nil, nil, fmt.Errorf("failed to parse channel info: %w", err)
			}
			if _, ok := it.channels[channelInfo.ChannelID]; !ok {
				if len(it.topics) == 0 || it.topics[channelInfo.TopicName] {
					it.channels[channelInfo.ChannelID] = channelInfo
				}
			}
		case TokenMessage:
			message := parseMessage(token.bytes())
			if _, ok := it.channels[message.ChannelID]; !ok {
				// skip messages on channels we don't know about. Note that if
				// an unindexed reader encounters a message it would be
				// interested in, but has not yet encountered the corresponding
				// channel ID, it has no option but to skip.
				continue
			}
			if message.RecordTime >= uint64(it.start) && message.RecordTime < uint64(it.end) {
				return it.channels[message.ChannelID], message, nil
			}
		default:
			_, err := io.CopyN(io.Discard, token.Reader, token.ByteCount)
			if err != nil {
				return nil, nil, err
			}
		}
	}
}
