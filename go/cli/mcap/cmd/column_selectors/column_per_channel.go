package column_selectors

import (
	"fmt"
	"io"

	"github.com/foxglove/mcap/go/mcap"
)

type columnPerChannelSelector struct {
	channels []*mcap.Channel
}

func NewColumnPerChannelSelector(rs io.ReadSeeker) (mcap.ColumnSelector, error) {
	reader, err := mcap.NewReader(rs)
	if err != nil {
		return nil, err
	}
	info, err := reader.Info()
	if err != nil {
		return nil, err
	}
	selector := &columnPerChannelSelector{}
	for _, channel := range info.Channels {
		selector.channels = append(selector.channels, channel)
	}
	return selector, nil
}

func (s *columnPerChannelSelector) ColumnForChannel(c *mcap.Channel) (int, error) {
	return int(c.ID), nil
}
func (m *columnPerChannelSelector) ColumnForSchema(s *mcap.Schema) (int, error) {
	for _, channel := range m.channels {
		if channel.SchemaID == s.ID {
			return int(channel.ID), nil
		}
	}
	return 0, fmt.Errorf("no known channel for schema with id %d", s.ID)
}

func (m *columnPerChannelSelector) ColumnForMessage(message *mcap.Message) (int, error) {
	return int(message.ChannelID), nil
}
