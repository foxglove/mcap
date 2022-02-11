package cmd

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"time"

	"github.com/foxglove/mcap/go/libmcap"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

var (
	LongAgo = time.Now().Add(-20 * 365 * 24 * time.Hour)
)

func printInfo(w io.Writer, info *libmcap.Info) error {
	buf := &bytes.Buffer{}

	fmt.Fprintf(buf, "messages: %d\n", info.Statistics.MessageCount)

	start := info.Statistics.MessageStartTime
	end := info.Statistics.MessageEndTime
	starttime := time.Unix(int64(start/1e9), int64(start%1e9))
	endtime := time.Unix(int64(end/1e9), int64(end%1e9))
	fmt.Fprintf(buf, "duration: %s\n", endtime.Sub(starttime))
	if starttime.After(LongAgo) {
		fmt.Fprintf(buf, "start: %s\n", starttime.Format(time.RFC3339Nano))
		fmt.Fprintf(buf, "end: %s\n", endtime.Format(time.RFC3339Nano))
	} else {
		fmt.Fprintf(buf, "start: %.3f\n", float64(starttime.UnixNano())/1e9)
		fmt.Fprintf(buf, "end: %.3f\n", float64(endtime.UnixNano())/1e9)
	}

	if len(info.ChunkIndexes) > 0 {
		compressionFormatStats := make(map[libmcap.CompressionFormat]struct {
			count            int
			compressedSize   uint64
			uncompressedSize uint64
		})
		for _, ci := range info.ChunkIndexes {
			stats := compressionFormatStats[ci.Compression]
			stats.count++
			stats.compressedSize += ci.CompressedSize
			stats.uncompressedSize += ci.UncompressedSize
			compressionFormatStats[ci.Compression] = stats
		}
		fmt.Fprintf(buf, "chunks:\n")
		chunkCount := len(info.ChunkIndexes)
		for k, v := range compressionFormatStats {
			compressionRatio := 100 * (1 - float64(v.compressedSize)/float64(v.uncompressedSize))
			fmt.Fprintf(buf, "\t%s: [%d/%d chunks] (%.2f%%) \n", k, v.count, chunkCount, compressionRatio)
		}
	}

	fmt.Fprintf(buf, "channels:\n")

	chanIDs := []uint16{}
	for chanID := range info.Channels {
		chanIDs = append(chanIDs, chanID)
	}
	sort.Slice(chanIDs, func(i, j int) bool {
		return chanIDs[i] < chanIDs[j]
	})
	rows := [][]string{}

	maxCountWidth := 0
	for _, v := range info.Statistics.ChannelMessageCounts {
		count := fmt.Sprintf("%d", v)
		if len(count) > maxCountWidth {
			maxCountWidth = len(count)
		}
	}

	for _, chanID := range chanIDs {
		channel := info.Channels[chanID]
		schema := info.Schemas[channel.SchemaID]
		channelMessageCount := info.Statistics.ChannelMessageCounts[chanID]
		row := []string{
			fmt.Sprintf("\t(%d) %s", channel.ID, channel.Topic),
			fmt.Sprintf("%*d msgs", maxCountWidth, channelMessageCount),
			fmt.Sprintf(" : %s [%s]", schema.Name, schema.Encoding),
		}
		rows = append(rows, row)
	}
	tw := tablewriter.NewWriter(buf)
	tw.SetBorder(false)
	tw.SetAutoWrapText(false)
	tw.SetAlignment(tablewriter.ALIGN_LEFT)
	tw.SetColumnSeparator("")
	tw.AppendBulk(rows)
	tw.Render()

	fmt.Fprintf(buf, "attachments: %d\n", info.Statistics.AttachmentCount)
	_, err := buf.WriteTo(w)
	return err
}

var infoCmd = &cobra.Command{
	Use:   "info",
	Short: "Report statistics about an mcap file",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			log.Fatal("Unexpected number of args")
		}
		r, err := os.Open(args[0])
		if err != nil {
			log.Fatal(err)
		}
		reader, err := libmcap.NewReader(r)
		if err != nil {
			log.Fatal(err)
		}
		info, err := reader.Info()
		if err != nil {
			log.Fatal(err)
		}
		err = printInfo(os.Stdout, info)
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(infoCmd)
}
