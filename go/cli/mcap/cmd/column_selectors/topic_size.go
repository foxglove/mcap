package column_selectors

import (
	"fmt"
	"io"
	"math"

	"github.com/foxglove/mcap/go/mcap"
)

type topicSizeClassSelector struct {
	channels []channelIndexPair
}

func NewTopicSizeClassSelector(rs io.ReadSeeker) (mcap.ColumnSelector, error) {
	reader, err := mcap.NewReader(rs)
	if err != nil {
		return nil, err
	}
	info, err := reader.Info()
	if err != nil {
		return nil, err
	}
	topicAverageSizes, err := getTopicAverageSizes(reader)
	if err != nil {
		return nil, err
	}
	selector := &topicSizeClassSelector{}
	for topicName, averageSize := range topicAverageSizes {
		for _, channel := range info.Channels {
			if channel.Topic == topicName {
				// All messages below 1kb are grouped into one column.
				columnIndex := int(math.Log2(averageSize)) - 10
				if columnIndex < 0 {
					columnIndex = 0
				}
				selector.channels = append(selector.channels, channelIndexPair{
					channel: channel,
					index:   columnIndex,
				})
			}
		}
	}
	return selector, nil
}

func (s *topicSizeClassSelector) ColumnForChannel(channel *mcap.Channel) (int, error) {
	for _, pair := range s.channels {
		if pair.channel.ID == channel.ID {
			return pair.index, nil
		}
	}
	return 0, fmt.Errorf("unrecognized channel with ID %d", channel.ID)
}

func (s *topicSizeClassSelector) ColumnForSchema(schema *mcap.Schema) (int, error) {
	for _, pair := range s.channels {
		if pair.channel.SchemaID == schema.ID {
			return pair.index, nil
		}
	}
	return 0, fmt.Errorf("unrecognized schema ID: %d", schema.ID)
}

func (s *topicSizeClassSelector) ColumnForMessage(message *mcap.Message) (int, error) {
	for _, pair := range s.channels {
		if pair.channel.ID == message.ChannelID {
			return pair.index, nil
		}
	}
	return 0, fmt.Errorf("message with unrecognized channel ID: %d", message.ChannelID)
}
