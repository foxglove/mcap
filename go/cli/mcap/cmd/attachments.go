package cmd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

func printAttachments(w io.Writer, attachmentIndexes []*mcap.AttachmentIndex) {
	rows := make([][]string, 0, len(attachmentIndexes))
	rows = append(rows, []string{
		"name",
		"media type",
		"log time",
		"creation time",
		"content length",
		"offset",
	})
	for _, idx := range attachmentIndexes {
		row := []string{
			idx.Name,
			idx.MediaType,
			fmt.Sprintf("%d", idx.LogTime),
			fmt.Sprintf("%d", idx.CreateTime),
			fmt.Sprintf("%d", idx.DataSize),
			fmt.Sprintf("%d", idx.Offset),
		}
		rows = append(rows, row)
	}
	utils.FormatTable(w, rows)
}

var attachmentsCmd = &cobra.Command{
	Use:   "attachments",
	Short: "List attachments in an MCAP file",
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
			printAttachments(os.Stdout, info.AttachmentIndexes)
			return nil
		})
		if err != nil {
			die("Failed to list attachments: %s", err)
		}
	},
}

func init() {
	listCmd.AddCommand(attachmentsCmd)
}
