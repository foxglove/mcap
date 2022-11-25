package column_selectors

import (
	"io"

	"github.com/foxglove/mcap/go/mcap"
)

type manualTopicSelector struct {
	channels        []channelIndexPair
	remainderColumn int
}

func NewManualTopicColumnSelector(rs io.ReadSeeker, topics []string) (mcap.ColumnSelector, error) {
	reader, err := mcap.NewReader(rs)
	if err != nil {
		return nil, err
	}
	info, err := reader.Info()
	if err != nil {
		return nil, err
	}
	selector := &manualTopicSelector{}
	for _, channel := range info.Channels {
		for i, topic := range topics {
			if topic == channel.Topic {
				selector.channels = append(selector.channels, channelIndexPair{channel, i})
			}
		}
	}
	selector.remainderColumn = len(topics)
	return selector, nil
}

func (m *manualTopicSelector) ColumnForChannel(c *mcap.Channel) (int, error) {
	for _, pair := range m.channels {
		if pair.channel.ID == c.ID {
			return pair.index, nil
		}
	}
	return m.remainderColumn, nil
}
func (m *manualTopicSelector) ColumnForSchema(s *mcap.Schema) (int, error) {
	for _, pair := range m.channels {
		if pair.channel.SchemaID == s.ID {
			return pair.index, nil
		}
	}
	return m.remainderColumn, nil
}

func (m *manualTopicSelector) ColumnForMessage(message *mcap.Message) (int, error) {
	for _, pair := range m.channels {
		if pair.channel.ID == message.ChannelID {
			return pair.index, nil
		}
	}
	return m.remainderColumn, nil
}
