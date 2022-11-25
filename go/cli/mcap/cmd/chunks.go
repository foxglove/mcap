package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

func printChunks(w io.Writer, chunkIndexes []*mcap.ChunkIndex, chunkTopics [][]string) {
	rows := make([][]string, 0, len(chunkIndexes))
	rows = append(rows, []string{
		"offset",
		"length",
		"start",
		"end",
		"compression",
		"compressed size",
		"uncompressed size",
		"compression ratio",
		"message index length",
		"topics in chunk",
	})
	for i, ci := range chunkIndexes {
		ratio := float64(ci.CompressedSize) / float64(ci.UncompressedSize)
		row := []string{
			fmt.Sprintf("%d", ci.ChunkStartOffset),
			fmt.Sprintf("%d", ci.ChunkLength),
			fmt.Sprintf("%d", ci.MessageStartTime),
			fmt.Sprintf("%d", ci.MessageEndTime),
			string(ci.Compression),
			fmt.Sprintf("%d", ci.CompressedSize),
			fmt.Sprintf("%d", ci.UncompressedSize),
			fmt.Sprintf("%f", ratio),
			fmt.Sprintf("%d", ci.MessageIndexLength),
			strings.Join(chunkTopics[i], ", "),
		}
		rows = append(rows, row)
	}
	utils.FormatTable(w, rows)
}

func determineChunkTopics(info *mcap.Info) [][]string {
	topicsPerChunk := make([][]string, len(info.ChunkIndexes))
	for i, ci := range info.ChunkIndexes {
		for channelID, _ := range ci.MessageIndexOffsets {
			for _, channel := range info.Channels {
				if channel.ID == channelID {
					topicsPerChunk[i] = append(topicsPerChunk[i], channel.Topic)
				}
			}
		}
		sort.Strings(topicsPerChunk[i])
	}
	return topicsPerChunk
}

// chunksCmd represents the chunks command
var chunksCmd = &cobra.Command{
	Use:   "chunks",
	Short: "List chunks in an MCAP file",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		if len(args) != 1 {
			die("Unexpected number of args")
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
			chunkTopics := determineChunkTopics(info)
			printChunks(os.Stdout, info.ChunkIndexes, chunkTopics)
			return nil
		})
		if err != nil {
			die("Failed to list chunks: %s", err)
		}
	},
}

func init() {
	listCmd.AddCommand(chunksCmd)
}
