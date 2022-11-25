package column_selectors

import (
	"fmt"
	"io"

	"github.com/foxglove/mcap/go/mcap"
)

type columnPerSchemaSelector struct {
	channels []*mcap.Channel
}

func NewColumnPerSchemaSelector(rs io.ReadSeeker) (mcap.ColumnSelector, error) {
	reader, err := mcap.NewReader(rs)
	if err != nil {
		return nil, err
	}
	info, err := reader.Info()
	if err != nil {
		return nil, err
	}
	selector := &columnPerSchemaSelector{}
	for _, channel := range info.Channels {
		selector.channels = append(selector.channels, channel)
	}
	return selector, nil
}

func (s *columnPerSchemaSelector) ColumnForChannel(c *mcap.Channel) (int, error) {
	return int(c.SchemaID), nil
}
func (s *columnPerSchemaSelector) ColumnForSchema(schema *mcap.Schema) (int, error) {
	return int(schema.ID), nil
}

func (s *columnPerSchemaSelector) ColumnForMessage(message *mcap.Message) (int, error) {
	for _, channel := range s.channels {
		if channel.ID == message.ChannelID {
			return int(channel.SchemaID), nil
		}
	}
	return 0, fmt.Errorf("message has unrecognized channel ID: %d", message.ChannelID)
}
