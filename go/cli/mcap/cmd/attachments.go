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

func printAttachments(w io.Writer, r io.Reader) error {
	tw := tablewriter.NewWriter(w)
	rows := make([][]string, 0)
	rows = append(rows, []string{
		"log time",
		"name",
		"content type",
		"content length",
	})

	cbReader, err := mcap.NewCallbackReader(mcap.ReadOptions{
		OnAttachment: func(a *mcap.Attachment) error {
			row := []string{
				fmt.Sprintf("%d", a.LogTime),
				a.Name,
				a.ContentType,
				fmt.Sprintf("%d", len(a.Data)),
			}
			rows = append(rows, row)
			return nil
		},
	})
	if err != nil {
		return err
	}
	if err := cbReader.Read(r); err != nil {
		return err
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
			return printAttachments(os.Stdout, rs)
		})
		if err != nil {
			log.Fatal("Failed to list attachments: %w", err)
		}
	},
}

func init() {
	listCmd.AddCommand(attachmentsCmd)
}
