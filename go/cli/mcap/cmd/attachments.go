package cmd

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

func printAttachments(w io.Writer, attachmentIndexes []*mcap.AttachmentIndex) {
	tw := tablewriter.NewWriter(w)
	rows := make([][]string, 0, len(attachmentIndexes))
	rows = append(rows, []string{
		"log time",
		"name",
		"content type",
		"content length",
		"offset",
	})
	for _, idx := range attachmentIndexes {
		row := []string{
			fmt.Sprintf("%d", idx.LogTime),
			idx.Name,
			idx.ContentType,
			fmt.Sprintf("%d", idx.DataSize),
			fmt.Sprintf("%d", idx.Offset),
		}
		rows = append(rows, row)
	}
	tw.SetBorder(false)
	tw.SetAutoWrapText(false)
	tw.SetAlignment(tablewriter.ALIGN_LEFT)
	tw.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	tw.SetColumnSeparator("")
	tw.AppendBulk(rows)
	tw.Render()
}

var attachmentsCmd = &cobra.Command{
	Use:   "attachments",
	Short: "List attachments in an mcap file",
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
			printChunks(os.Stdout, info.ChunkIndexes)
			return nil
		})
		if err != nil {
			log.Fatal("Failed to list attachments: %w", err)
		}
	},
}

func init() {
	listCmd.AddCommand(attachmentsCmd)
}
