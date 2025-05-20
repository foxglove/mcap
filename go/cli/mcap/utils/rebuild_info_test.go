package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/require"
)

func dump(data interface{}) string {
	b, _ := json.MarshalIndent(data, "", "  ")
	return string(b)
}

func compareInfo(a, b *mcap.Info) error {
	if a == b {
		return nil
	}
	if a == nil || b == nil {
		return fmt.Errorf("one of the Info structs is nil")
	}
	if b.Statistics != nil {
		if !reflect.DeepEqual(a.Statistics, b.Statistics) {
			return fmt.Errorf("Statistics mismatch: %s != %s", dump(a.Statistics), dump(b.Statistics))
		}
	}
	if len(b.Channels) > 0 {
		if !reflect.DeepEqual(a.Channels, b.Channels) {
			return fmt.Errorf("Channels mismatch: %v != %v", a.Channels, b.Channels)
		}
	}
	if len(b.Schemas) > 0 {
		if !reflect.DeepEqual(a.Schemas, b.Schemas) {
			return fmt.Errorf("Schemas mismatch: %v != %v", a.Schemas, b.Schemas)
		}
	}
	if len(b.ChunkIndexes) > 0 {
		if !reflect.DeepEqual(a.ChunkIndexes, b.ChunkIndexes) {
			return fmt.Errorf("ChunkIndexes mismatch: %s != %s", dump(a.ChunkIndexes), dump(b.ChunkIndexes))
		}
	}
	if len(b.MetadataIndexes) > 0 {
		if !reflect.DeepEqual(a.MetadataIndexes, b.MetadataIndexes) {
			return fmt.Errorf("MetadataIndexes mismatch: %s != %s", dump(a.MetadataIndexes), dump(b.MetadataIndexes))
		}
	}
	if len(b.AttachmentIndexes) > 0 {
		if !reflect.DeepEqual(a.AttachmentIndexes, b.AttachmentIndexes) {
			return fmt.Errorf("AttachmentIndexes mismatch: %s != %s", dump(a.AttachmentIndexes), dump(b.AttachmentIndexes))
		}
	}

	return nil
}
func TestRebuild(t *testing.T) {
	inputs := []string{}
	err := filepath.Walk("../../../../tests/conformance/data/", func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && filepath.Ext(path) == ".mcap" {
			features := strings.Split(strings.TrimSuffix(info.Name(), filepath.Ext(info.Name())), "-")

			// Only check mcaps with either AttachmentIndexes, ChunkIndexes, or MetadataIndexes
			// and not with Padding
			if (slices.Contains(features, "ax") ||
				slices.Contains(features, "mdx") ||
				slices.Contains(features, "ch")) && !slices.Contains(features, "pad") {
				inputs = append(inputs, path)
			}
		}
		return nil
	})
	require.NoError(t, err)

	for _, path := range inputs {
		t.Run(path, func(t *testing.T) {
			input, err := os.ReadFile(path)
			require.NoError(t, err)
			r := bytes.NewReader(input)

			rebuildData, err := RebuildInfo(r, false)
			require.NoError(t, err)

			genInfo := rebuildData.Info

			r = bytes.NewReader(input)
			reader, err := mcap.NewReader(r)
			require.NoError(t, err)
			defer reader.Close()
			realInfo, err := reader.Info()
			require.NoError(t, err)

			err = compareInfo(genInfo, realInfo)
			require.NoError(t, err)
		})
	}
	require.NoError(t, err)
}
