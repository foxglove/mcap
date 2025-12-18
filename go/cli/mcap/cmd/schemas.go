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

func printDescriptorEnum(w io.Writer, enum *descriptorpb.EnumDescriptorProto, indent int) {
	spacer := strings.Repeat("  ", indent)
	fmt.Fprintf(w, "%senum %s {\n", spacer, enum.GetName())
	for _, value := range enum.GetValue() {
		fmt.Fprintf(w, "%s  %s = %d;\n", spacer, value.GetName(), value.GetNumber())
	}
	fmt.Fprintf(w, "%s}\n", spacer)
}

func printDescriptorMessage(w io.Writer, message *descriptorpb.DescriptorProto, indent int) {
	spacer := strings.Repeat("  ", indent)
	fmt.Fprintf(w, "%smessage %s {\n", spacer, message.GetName())
	for _, enum := range message.GetEnumType() {
		printDescriptorEnum(w, enum, indent+1)
	}
	for _, nested := range message.GetNestedType() {
		printDescriptorMessage(w, nested, indent+1)
	}
	for _, field := range message.GetField() {
		fieldType := field.GetTypeName()
		if fieldType == "" {
			fieldType = toType(field.GetType().String())
		}
		fmt.Fprintf(
			w,
			"%s  %s %s %s = %d;\n",
			spacer,
			toLabel(field.GetLabel().String()),
			fieldType,
			field.GetName(),
			field.GetNumber(),
		)
	}
	fmt.Fprintf(w, "%s}\n", spacer)
}

func printDescriptor(w io.Writer, desc *descriptorpb.FileDescriptorSet) {
	for i, file := range desc.File {
		if i != 0 {
			// add a separator between files
			fmt.Fprintf(w, "%s\n", strings.Repeat("-", 20))
		}
		fmt.Fprintf(w, "// file: %s\n", file.GetName())
		fmt.Fprintf(w, "syntax = \"%s\";\n", file.GetSyntax())
		fmt.Fprintf(w, "package %s;\n", file.GetPackage())
		for _, dependency := range file.GetDependency() {
			fmt.Fprintf(w, "import \"%s\";\n", dependency)
		}
		for _, enum := range file.GetEnumType() {
			printDescriptorEnum(w, enum, 0)
		}
		for _, message := range file.GetMessageType() {
			printDescriptorMessage(w, message, 0)
		}
	}
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
		case "ros1msg", "ros2msg", "ros2idl":
			displayString = string(schema.Data)
		case "protobuf":
			descriptor, err := parseDescriptor(schema.Data)
			if err != nil {
				die("failed to parse descriptor: %v", err)
			}
			buf := &bytes.Buffer{}
			printDescriptor(buf, descriptor)
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

// schemasCmd represents the schemas command.
var schemasCmd = &cobra.Command{
	Use:   "schemas",
	Short: "List schemas in an MCAP file",
	Run: func(_ *cobra.Command, args []string) {
		ctx := context.Background()
		if len(args) != 1 {
			die("Unexpected number of args")
		}
		filename := args[0]
		err := utils.WithReader(ctx, filename, func(_ bool, rs io.ReadSeeker) error {
			reader, err := mcap.NewReader(rs)
			if err != nil {
				return fmt.Errorf("failed to get reader: %w", err)
			}
			defer reader.Close()
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
