package cmd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var (
	LongAgo = time.Now().Add(-20 * 365 * 24 * time.Hour)
)

func decimalTime(t time.Time) string {
	unix := t.UnixNano()
	seconds := unix / 1e9
	nanoseconds := unix % 1e9
	return fmt.Sprintf("%d.%09d", seconds, nanoseconds)
}

func printInfo(w io.Writer, info *mcap.Info) error {
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "library: %s\n", info.Header.Library)
	fmt.Fprintf(buf, "profile: %s\n", info.Header.Profile)
	var start, end uint64
	durationInSeconds := float64(0)
	if info.Statistics != nil {
		fmt.Fprintf(buf, "messages: %d\n", info.Statistics.MessageCount)
		start = info.Statistics.MessageStartTime
		end = info.Statistics.MessageEndTime
		starttime := time.Unix(int64(start/1e9), int64(start%1e9))
		endtime := time.Unix(int64(end/1e9), int64(end%1e9))
		fmt.Fprintf(buf, "duration: %s\n", endtime.Sub(starttime))
		durationInSeconds = endtime.Sub(starttime).Seconds()
		if starttime.After(LongAgo) {
			fmt.Fprintf(buf, "start: %s (%s)\n", starttime.Format(time.RFC3339Nano), decimalTime(starttime))
			fmt.Fprintf(buf, "end: %s (%s)\n", endtime.Format(time.RFC3339Nano), decimalTime(endtime))
		} else {
			fmt.Fprintf(buf, "start: %.3f\n", float64(starttime.UnixNano())/1e9)
			fmt.Fprintf(buf, "end: %.3f\n", float64(endtime.UnixNano())/1e9)
		}
	}
	if len(info.ChunkIndexes) > 0 {
		compressionFormatStats := make(map[mcap.CompressionFormat]struct {
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
		fmt.Fprintf(buf, "compression:\n")
		totalCompressedSize := uint64(0)
		totalUncompressedSize := uint64(0)
		chunkCount := len(info.ChunkIndexes)
		for k, v := range compressionFormatStats {
			compressionRatio := 100 * (1 - float64(v.compressedSize)/float64(v.uncompressedSize))
			fmt.Fprintf(buf, "\t%s: [%d/%d chunks] (%.2f%%) \n", k, v.count, chunkCount, compressionRatio)
			totalCompressedSize += v.compressedSize
			totalUncompressedSize += v.uncompressedSize
		}

		fmt.Fprintf(buf, "uncompressed: %.2d MB", totalUncompressedSize/(1024*1024))
		if durationInSeconds > 0 {
			fmt.Fprintf(buf, " @ %.2f MB/sec", float64(totalUncompressedSize/(1024*1024))/durationInSeconds)
		}
		fmt.Fprintf(buf, "\n")

		fmt.Fprintf(buf, "compressed: %.2d MB", totalCompressedSize/(1024*1024))
		if durationInSeconds > 0 {
			fmt.Fprintf(buf, " @ %.2f MB/sec", float64(totalCompressedSize/(1024*1024))/durationInSeconds)
		}

		totalCompressionRatio := 100 * (1 - float64(totalCompressedSize)/float64(totalUncompressedSize))
		fmt.Fprintf(buf, " (%.2f%%) \n", totalCompressionRatio)
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
	if info.Statistics != nil {
		for _, v := range info.Statistics.ChannelMessageCounts {
			count := fmt.Sprintf("%d", v)
			if len(count) > maxCountWidth {
				maxCountWidth = len(count)
			}
		}
	}
	for _, chanID := range chanIDs {
		channel := info.Channels[chanID]
		schema := info.Schemas[channel.SchemaID]
		channelMessageCount := info.Statistics.ChannelMessageCounts[chanID]
		frequency := 1e9 * float64(channelMessageCount) / float64(end-start)
		row := []string{
			fmt.Sprintf("\t(%d) %s", channel.ID, channel.Topic),
		}
		if info.Statistics != nil {
			row = append(row, fmt.Sprintf("%*d msgs (%.2f Hz)", maxCountWidth, channelMessageCount, frequency))
		}
		if schema != nil {
			row = append(row, fmt.Sprintf(" : %s [%s]", schema.Name, schema.Encoding))
		} else if channel.SchemaID != 0 {
			row = append(row, fmt.Sprintf(" : <missing schema %d>", channel.SchemaID))
		} else {
			row = append(row, " : <no schema>")
		}
		rows = append(rows, row)
	}
	utils.FormatTable(buf, rows)
	var attachmentCount, metadataCount uint32
	if info.Statistics != nil {
		attachmentCount = info.Statistics.AttachmentCount
		metadataCount = info.Statistics.MetadataCount
	}
	fmt.Fprintf(buf, "attachments: %d\n", attachmentCount)
	fmt.Fprintf(buf, "metadata: %d\n", metadataCount)
	_, err := buf.WriteTo(w)
	return err
}

var infoCmd = &cobra.Command{
	Use:   "info",
	Short: "Report statistics about an MCAP file",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		if len(args) != 1 {
			die("Unexpected number of args")
		}
		// check if it's a remote file
		filename := args[0]
		err := utils.WithReader(ctx, filename, func(remote bool, rs io.ReadSeeker) error {
			reader, err := mcap.NewReader(rs)
			if err != nil {
				return fmt.Errorf("failed to get reader: %w", err)
			}
			info, err := reader.Info()
			if err != nil {
				return fmt.Errorf("failed to get info: %w", err)
			}
			err = printInfo(os.Stdout, info)
			if err != nil {
				return fmt.Errorf("failed to print info: %w", err)
			}
			return nil
		})
		if err != nil {
			die("Failed to read file %s: %v", filename, err)
		}
	},
}

func init() {
	rootCmd.AddCommand(infoCmd)
}
