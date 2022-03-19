package cmd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb" // cspell:words descriptorpb
)

var (
	protobufSchemaSeparator = "================================================================================\n"
)

func parseDescriptor(b []byte) (*descriptorpb.FileDescriptorSet, error) {
	descriptor := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(b, descriptor); err != nil {
		return nil, err
	}
	return descriptor, nil
}

func printDescriptor(desc *descriptorpb.FileDescriptorSet) string {
	buf := &bytes.Buffer{}
	for i, file := range desc.File {
		if i > 0 {
			fmt.Fprint(buf, protobufSchemaSeparator)
		}
		fmt.Fprintln(buf, "file:", strings.TrimSpace(file.GetName()))
		for _, descriptor := range file.MessageType {
			for _, field := range descriptor.Field {
				fieldType := field.GetType()
				fmt.Fprintf(buf, "  %s %s\n", field.GetName(), fieldType.String())
			}
		}
	}
	return buf.String()
}

func printSchemas(w io.Writer, schemas []*mcap.Schema) {
	tw := tablewriter.NewWriter(w)
	rows := make([][]string, 0, len(schemas))
	rows = append(rows, []string{
		"id",
		"name",
		"encoding",
		"data",
	})
	for _, schema := range schemas {

		var displayString string
		switch schema.Encoding {
		case "ros1msg", "ros2msg":
			displayString = string(schema.Data)
		case "protobuf":
			descriptor, err := parseDescriptor(schema.Data)
			if err != nil {
				log.Fatalf("failed to parse descriptor: %v", err)
			}
			displayString = printDescriptor(descriptor)
		default:
			displayString = string(schema.Data)
		}

		row := []string{
			fmt.Sprintf("%d", schema.ID),
			schema.Name,
			schema.Encoding,
			displayString,
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

// schemasCmd represents the schemas command
var schemasCmd = &cobra.Command{
	Use:   "schemas",
	Short: "List schemas in an mcap file",
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

			schemas := []*mcap.Schema{}
			for _, schema := range info.Schemas {
				schemas = append(schemas, schema)
			}
			sort.Slice(schemas, func(i, j int) bool {
				return schemas[i].ID < schemas[j].ID
			})
			printSchemas(os.Stdout, schemas)
			return nil
		})
		if err != nil {
			log.Fatal("Failed to list schemas: %w", err)
		}
	},
}

func init() {
	listCmd.AddCommand(schemasCmd)
}
