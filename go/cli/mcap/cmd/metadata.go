package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

func printMetadata(w io.Writer, r io.ReadSeeker, info *mcap.Info) error {
	tw := tablewriter.NewWriter(w)
	rows := make([][]string, 0, len(info.MetadataIndexes))
	rows = append(rows, []string{
		"name",
		"offset",
		"length",
		"metadata",
	})
	for _, idx := range info.MetadataIndexes {
		offset := idx.Offset + 1 + 8
		if offset > math.MaxInt64 {
			return fmt.Errorf("metadata offset out of range: %v", offset)
		}
		_, err := r.Seek(int64(offset), io.SeekStart)
		if err != nil {
			return fmt.Errorf("failed to seek to metadata record: %w", err)
		}
		record := make([]byte, idx.Length)
		_, err = r.Read(record)
		if err != nil {
			return fmt.Errorf("failed to read metadata record: %w", err)
		}
		metadata, err := mcap.ParseMetadata(record)
		if err != nil {
			return fmt.Errorf("failed to parse metadata: %w", err)
		}

		jsonSerialized, err := json.Marshal(metadata.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata to JSON: %w", err)
		}

		indented := &bytes.Buffer{}
		err = json.Indent(indented, jsonSerialized, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to indent JSON: %w", err)
		}
		rows = append(rows, []string{
			idx.Name,
			fmt.Sprintf("%d", idx.Offset),
			fmt.Sprintf("%d", idx.Length),
			indented.String(),
		})
	}
	tw.SetBorder(false)
	tw.SetAutoWrapText(false)
	tw.SetAlignment(tablewriter.ALIGN_LEFT)
	tw.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	tw.SetColumnSeparator("")
	tw.AppendBulk(rows)
	tw.Render()
	return nil
}

var listMetadataCmd = &cobra.Command{
	Use:   "metadata",
	Short: "List metadata in an mcap file",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		if len(args) != 1 {
			die("Unexpected number of args")
		}
		filename := args[0]
		err := utils.WithReader(ctx, filename, func(matched bool, rs io.ReadSeeker) error {
			reader, err := mcap.NewReader(rs)
			if err != nil {
				return fmt.Errorf("failed to build mcap reader: %w", err)
			}
			info, err := reader.Info()
			if err != nil {
				return fmt.Errorf("failed to read info: %w", err)
			}
			return printMetadata(os.Stdout, rs, info)
		})
		if err != nil {
			die("failed to list metadata: %s", err)
		}
	},
}

func init() {
	listCmd.AddCommand(listMetadataCmd)
}
