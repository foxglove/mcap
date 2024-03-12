package cmd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var (
	LongAgo = time.Now().Add(-20 * 365 * 24 * time.Hour)
)

func decimalTime(t time.Time) string {
	seconds := t.Unix()
	nanoseconds := t.Nanosecond()
	return fmt.Sprintf("%d.%09d", seconds, nanoseconds)
}

func humanBytes(numBytes uint64) string {
	prefixes := []string{"B", "KiB", "MiB", "GiB"}

	for index, p := range prefixes {
		displayedValue := float64(numBytes) / (math.Pow(1024, float64(index)))
		if displayedValue <= 1024 {
			return fmt.Sprintf("%.2f %s", displayedValue, p)
		}
	}
	lastIndex := len(prefixes) - 1
	displayedValue := float64(numBytes) / (math.Pow(1024, float64(lastIndex)))
	return fmt.Sprintf("%.2f %s", displayedValue, prefixes[lastIndex])
}

func getDurationNs(start uint64, end uint64) float64 {
	// subtracts start from end, returning the result as a 64-bit float.
	// float64 can represent the entire range of possible durations (-2**64, 2**64),
	// albeit with some loss of precision.
	diff := end - start
	signMultiplier := 1.0
	if start > end {
		diff = start - end
		signMultiplier = -1.0
	}
	return float64(diff) * signMultiplier
}

func addRow(rows [][]string, field string, value string, args ...any) [][]string {
	return append(rows, []string{field, fmt.Sprintf(value, args...)})
}

func printInfo(w io.Writer, info *mcap.Info) error {
	buf := &bytes.Buffer{}

	header := [][]string{
		{"library:", info.Header.Library},
		{"profile:", info.Header.Profile},
	}
	var start, end uint64
	durationInSeconds := float64(0)
	if info.Statistics != nil {
		header = addRow(header, "messages:", "%d", info.Statistics.MessageCount)
		start = info.Statistics.MessageStartTime
		end = info.Statistics.MessageEndTime
		durationNs := getDurationNs(start, end)
		durationInSeconds = durationNs / 1e9
		starttime := time.Unix(int64(start/1e9), int64(start%1e9))
		endtime := time.Unix(int64(end/1e9), int64(end%1e9))
		if math.Abs(durationNs) > math.MaxInt64 {
			// time.Duration is an int64 nanosecond count under the hood, but end and start can
			// be further apart than that.
			header = addRow(header, "duration:", "%.3fs", durationInSeconds)
		} else {
			header = addRow(header, "duration:", "%s", endtime.Sub(starttime))
		}
		if starttime.After(LongAgo) {
			header = addRow(header, "start:", "%s (%s)", starttime.Format(time.RFC3339Nano), decimalTime(starttime))
			header = addRow(header, "end:", "%s (%s)", endtime.Format(time.RFC3339Nano), decimalTime(endtime))
		} else {
			header = addRow(header, "start:", "%s", decimalTime(starttime))
			header = addRow(header, "end:", "%s", decimalTime(endtime))
		}
	}
	utils.FormatTable(buf, header)
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
		chunkCount := len(info.ChunkIndexes)
		for k, v := range compressionFormatStats {
			compressionRatio := 100 * (1 - float64(v.compressedSize)/float64(v.uncompressedSize))
			fmt.Fprintf(buf, "\t%s: [%d/%d chunks] ", k, v.count, chunkCount)
			fmt.Fprintf(buf, "[%s/%s (%.2f%%)] ",
				humanBytes(v.uncompressedSize), humanBytes(v.compressedSize), compressionRatio)
			if durationInSeconds > 0 {
				fmt.Fprintf(buf, "[%s/sec] ", humanBytes(uint64(float64(v.compressedSize)/durationInSeconds)))
			}
			fmt.Fprintf(buf, "\n")
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
	if info.Statistics != nil {
		for _, v := range info.Statistics.ChannelMessageCounts {
			count := fmt.Sprintf("%d", v)
			if len(count) > maxCountWidth {
				maxCountWidth = len(count)
			}
		}
	}

	maxChanIDWidth := 0
	if len(chanIDs) > 0 {
		maxChanIDWidth = digits(uint64(chanIDs[len(chanIDs)-1])) + 3
	}
	for _, chanID := range chanIDs {
		channel := info.Channels[chanID]
		schema := info.Schemas[channel.SchemaID]
		width := digits(uint64(chanID)) + 2
		padding := strings.Repeat(" ", maxChanIDWidth-width)
		row := []string{
			fmt.Sprintf("\t(%d)%s%s", channel.ID, padding, channel.Topic),
		}
		if info.Statistics != nil {
			channelMessageCount := info.Statistics.ChannelMessageCounts[chanID]
			if channelMessageCount > 1 {
				frequency := 1e9 * float64(channelMessageCount) / float64(end-start)
				row = append(row, fmt.Sprintf("%*d msgs (%.2f Hz)", maxCountWidth, channelMessageCount, frequency))
			} else {
				row = append(row, fmt.Sprintf("%*d msgs", maxCountWidth, channelMessageCount))
			}
		}
		switch {
		case schema != nil:
			row = append(row, fmt.Sprintf(" : %s [%s]", schema.Name, schema.Encoding))
		case channel.SchemaID != 0:
			row = append(row, fmt.Sprintf(" : <missing schema %d>", channel.SchemaID))
		default:
			row = append(row, " : <no schema>")
		}
		rows = append(rows, row)
	}
	utils.FormatTable(buf, rows)
	if info.Statistics != nil {
		fmt.Fprintf(buf, "attachments: %d\n", info.Statistics.AttachmentCount)
		fmt.Fprintf(buf, "metadata: %d\n", info.Statistics.MetadataCount)
	} else {
		fmt.Fprintf(buf, "attachments: unknown\n")
		fmt.Fprintf(buf, "metadata: unknown\n")
	}
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
			defer reader.Close()
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
