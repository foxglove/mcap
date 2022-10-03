package cmd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb" // cspell:words descriptorpb
)

func parseDescriptor(b []byte) (*descriptorpb.FileDescriptorSet, error) {
	descriptor := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(b, descriptor); err != nil {
		return nil, err
	}
	return descriptor, nil
}

func toLabel(s string) string {
	return strings.ToLower(strings.TrimPrefix(s, "LABEL_"))
}

func toType(s string) string {
	return strings.ToLower(strings.TrimPrefix(s, "TYPE_"))
}

func printDescriptor(w io.Writer, desc *descriptorpb.FileDescriptorSet) error {
	for i, file := range desc.File {
		if i == 0 {
			fmt.Fprintf(w, "syntax = \"%s\";\n\n", file.GetSyntax())
		}
		for _, message := range file.GetMessageType() {
			fmt.Fprintf(w, "message %s.%s {\n", file.GetPackage(), message.GetName())
			for _, field := range message.GetField() {
				fieldType := field.GetTypeName()
				if fieldType == "" {
					fieldType = toType(field.GetType().String())
				}
				fmt.Fprintf(w, "  %s %s %s = %d;\n", toLabel(field.GetLabel().String()), field.GetName(), fieldType, field.GetNumber())
			}
			fmt.Fprintf(w, "}\n")
		}
	}
	return nil
}

func printSchemas(w io.Writer, schemas []*mcap.Schema) {
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
				die("failed to parse descriptor: %v", err)
			}
			buf := &bytes.Buffer{}
			err = printDescriptor(buf, descriptor)
			if err != nil {
				die("Failed to print descriptor: %v", err)
			}
			displayString = buf.String()
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
	utils.FormatTable(w, rows)
}

// schemasCmd represents the schemas command
var schemasCmd = &cobra.Command{
	Use:   "schemas",
	Short: "List schemas in an MCAP file",
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
			die("Failed to list schemas: %s", err)
		}
	},
}

func init() {
	listCmd.AddCommand(schemasCmd)
}
