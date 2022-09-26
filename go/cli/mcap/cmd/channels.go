package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

func printChannels(w io.Writer, channels []*mcap.Channel) error {
	rows := make([][]string, 0, len(channels))
	rows = append(rows, []string{
		"id",
		"schemaId",
		"topic",
		"messageEncoding",
		"metadata",
	})
	for _, channel := range channels {
		metadata, err := json.Marshal(channel.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal channel metadata: %v", err)
		}
		row := []string{
			fmt.Sprintf("%d", channel.ID),
			fmt.Sprintf("%d", channel.SchemaID),
			channel.Topic,
			channel.MessageEncoding,
			string(metadata),
		}
		rows = append(rows, row)
	}
	utils.FormatTable(w, rows)
	return nil
}

// channelsCmd represents the channels command
var channelsCmd = &cobra.Command{
	Use:   "channels",
	Short: "List channels in an mcap file",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		if len(args) != 1 {
			log.Fatal("Unexpected number of args")
		}
		filename := args[0]
		err := utils.WithReader(ctx, filename, func(matched bool, rs io.ReadSeeker) error {
			reader, err := mcap.NewReader(rs)
			if err != nil {
				return fmt.Errorf("failed to get reader: %w", err)
			}
			info, err := reader.Info()
			if err != nil {
				return fmt.Errorf("failed to get info: %w", err)
			}
			channels := []*mcap.Channel{}
			for _, channel := range info.Channels {
				channels = append(channels, channel)
			}
			sort.Slice(channels, func(i, j int) bool {
				return channels[i].ID < channels[j].ID
			})
			return printChannels(os.Stdout, channels)
		})
		if err != nil {
			log.Fatal("Failed to list channels: %w", err)
		}
	},
}

func init() {
	listCmd.AddCommand(channelsCmd)
}
