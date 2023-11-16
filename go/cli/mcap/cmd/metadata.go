package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var (
	addMetadataKeyValues []string
	addMetadataName      string
)

var (
	getMetadataName string
)

func printMetadata(w io.Writer, r io.ReadSeeker, info *mcap.Info) error {
	rows := make([][]string, 0, len(info.MetadataIndexes))
	headers := []string{
		"name",
		"offset",
		"length",
		"metadata",
	}
	rows = append(rows, headers)
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
		rows = append(rows, []string{
			idx.Name,
			fmt.Sprintf("%d", idx.Offset),
			fmt.Sprintf("%d", idx.Length),
      string(jsonSerialized),
		})
	}
	utils.FormatTable(w, rows)
	return nil
}

var listMetadataCmd = &cobra.Command{
	Use:   "metadata",
	Short: "List metadata in an MCAP file",
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
			defer reader.Close()
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

var addMetadataCmd = &cobra.Command{
	Use:   "metadata",
	Short: "Add metadata to an MCAP file",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("Unexpected number of args")
		}
		filename := args[0]

		f, err := os.OpenFile(filename, os.O_RDWR, os.ModePerm)
		if err != nil {
			die("failed to open file: %s", err)
		}
		defer f.Close()

		metadata := make(map[string]string)
		for _, kv := range addMetadataKeyValues {
			parts := strings.FieldsFunc(kv, func(c rune) bool {
				return c == '='
			})
			if len(parts) != 2 {
				die("failed to parse key/value %s", kv)
			}
			metadata[parts[0]] = parts[1]
		}
		err = utils.AmendMCAP(f,
			nil,
			[]*mcap.Metadata{
				{
					Name:     addMetadataName,
					Metadata: metadata,
				},
			},
		)
		if err != nil {
			die("failed to add metadata: %s. You may need to run `mcap recover` to repair the file.", err)
		}
	},
}

var getMetadataCmd = &cobra.Command{
	Use:   "metadata",
	Short: "get metadata by name",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		if len(args) != 1 {
			die("Unexpected number of args")
		}
		filename := args[0]
		err := utils.WithReader(ctx, filename, func(_ bool, rs io.ReadSeeker) error {
			reader, err := mcap.NewReader(rs)
			if err != nil {
				return fmt.Errorf("failed to build reader: %w", err)
			}
			defer reader.Close()
			info, err := reader.Info()
			if err != nil {
				return fmt.Errorf("failed to collect mcap info: %w", err)
			}

			output := make(map[string]string)

			metadataIndexes := make(map[string][]*mcap.MetadataIndex)
			for _, idx := range info.MetadataIndexes {
				metadataIndexes[idx.Name] = append(metadataIndexes[idx.Name], idx)
			}
			indexes, ok := metadataIndexes[getMetadataName]
			if !ok {
				return fmt.Errorf("metadata %s does not exist", getMetadataName)
			}

			for _, idx := range indexes {
				_, err = rs.Seek(int64(idx.Offset+1+8), io.SeekStart)
				if err != nil {
					return fmt.Errorf("failed to seek to metadata record at %d: %w", idx.Offset, err)
				}
				data := make([]byte, idx.Length)
				_, err = io.ReadFull(rs, data)
				if err != nil {
					return fmt.Errorf("failed to read metadata record: %w", err)
				}
				record, err := mcap.ParseMetadata(data)
				if err != nil {
					return fmt.Errorf("failed to parse metadata: %w", err)
				}
				for k, v := range record.Metadata {
					output[k] = v
				}
			}

			jsonBytes, err := json.Marshal(output)
			if err != nil {
				return fmt.Errorf("failed to marshal output to JSON: %w", err)
			}
			prettyJSON, err := utils.PrettyJSON(jsonBytes)
			if err != nil {
				return fmt.Errorf("failed to pretty JSON: %w", err)
			}
			_, err = os.Stdout.WriteString(prettyJSON + "\n")
			if err != nil {
				return fmt.Errorf("failed to write metadata to output: %w", err)
			}
			return nil
		})
		if err != nil {
			die("failed to fetch metadata: %s", err)
		}
	},
}

func init() {
	listCmd.AddCommand(listMetadataCmd)

	addCmd.AddCommand(addMetadataCmd)
	addMetadataCmd.PersistentFlags().StringVarP(&addMetadataName, "name", "n", "", "name of metadata record to add")
	addMetadataCmd.PersistentFlags().StringSliceVarP(&addMetadataKeyValues, "key", "k", []string{}, "key=value pair")
	err := addMetadataCmd.MarkPersistentFlagRequired("name")
	if err != nil {
		die("failed to mark --name flag as required: %s", err)
	}

	getCmd.AddCommand(getMetadataCmd)
	getMetadataCmd.PersistentFlags().StringVarP(&getMetadataName, "name", "n", "", "name of metadata record to create")
	err = getMetadataCmd.MarkPersistentFlagRequired("name")
	if err != nil {
		die("failed to mark --name flag as required: %s", err)
	}
}
