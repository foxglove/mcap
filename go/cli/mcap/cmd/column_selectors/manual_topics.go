package column_selectors

import (
	"io"

	"github.com/foxglove/mcap/go/mcap"
)

type manualTopicSelector struct {
	channels []*mcap.Channel
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
		for _, topic := range topics {
			if topic == channel.Topic {
				selector.channels = append(selector.channels, channel)
			}
		}
	}
	return selector, nil
}

func (m *manualTopicSelector) ColumnForChannel(c *mcap.Channel) (int, error) {
	for _, channel := range m.channels {
		if channel.ID == c.ID {
			return 1, nil
		}
	}
	return 0, nil
}
func (m *manualTopicSelector) ColumnForSchema(s *mcap.Schema) (int, error) {
	for _, channel := range m.channels {
		if channel.SchemaID == s.ID {
			return 1, nil
		}
	}
	return 0, nil
}

func (m *manualTopicSelector) ColumnForMessage(message *mcap.Message) (int, error) {
	for _, channel := range m.channels {
		if channel.ID == message.ChannelID {
			return 1, nil
		}
	}
	return 0, nil
}
