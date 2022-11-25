package column_selectors

import (
	"fmt"
	"io"

	"github.com/foxglove/mcap/go/mcap"
)

type topicSizeThresholdSelector struct {
	channels []channelIndexPair
}

func NewTopicSizeThresholdSelector(rs io.ReadSeeker, threshold int) (mcap.ColumnSelector, error) {
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
	selector := &topicSizeThresholdSelector{}
	for topicName, averageSize := range topicAverageSizes {
		for _, channel := range info.Channels {
			if channel.Topic == topicName {
				columnIndex := 0
				if averageSize > float64(threshold) {
					columnIndex = 1
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

func (s *topicSizeThresholdSelector) ColumnForChannel(channel *mcap.Channel) (int, error) {
	for _, pair := range s.channels {
		if pair.channel.ID == channel.ID {
			return pair.index, nil
		}
	}
	return 0, fmt.Errorf("unrecognized channel with ID %d", channel.ID)
}

func (s *topicSizeThresholdSelector) ColumnForSchema(schema *mcap.Schema) (int, error) {
	for _, pair := range s.channels {
		if pair.channel.SchemaID == schema.ID {
			return pair.index, nil
		}
	}
	return 0, fmt.Errorf("unrecognized schema ID: %d", schema.ID)
}

func (s *topicSizeThresholdSelector) ColumnForMessage(message *mcap.Message) (int, error) {
	for _, pair := range s.channels {
		if pair.channel.ID == message.ChannelID {
			return pair.index, nil
		}
	}
	return 0, fmt.Errorf("message with unrecognized channel ID: %d", message.ChannelID)
}
