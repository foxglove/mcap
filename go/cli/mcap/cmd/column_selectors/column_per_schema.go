package column_selectors

import (
	"fmt"

	"github.com/foxglove/mcap/go/mcap"
)

type columnPerSchemaSelector struct {
	channels []*mcap.Channel
}

func NewColumnPerSchemaSelector() mcap.ColumnSelector {
	return &columnPerSchemaSelector{}
}

func (s *columnPerSchemaSelector) ColumnForSchema(schema *mcap.Schema) (int, error) {
	return int(schema.ID), nil
}

func (s *columnPerSchemaSelector) ColumnForChannel(c *mcap.Channel) (int, error) {
	s.channels = append(s.channels, c)
	return int(c.SchemaID), nil
}

func (s *columnPerSchemaSelector) ColumnForMessage(message *mcap.Message) (int, error) {
	for _, channel := range s.channels {
		if channel.ID == message.ChannelID {
			return int(channel.SchemaID), nil
		}
	}
	return 0, fmt.Errorf("message has unrecognized channel ID: %d", message.ChannelID)
}
