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

func compareInfo(genInfo, realInfo *mcap.Info) error {
	if genInfo == realInfo {
		return nil
	}
	if genInfo == nil || realInfo == nil {
		return fmt.Errorf("one of the Info structs is nil")
	}
	if realInfo.Statistics != nil {
		if !reflect.DeepEqual(genInfo.Statistics, realInfo.Statistics) {
			return fmt.Errorf("Statistics mismatch: %s != %s", dump(genInfo.Statistics), dump(realInfo.Statistics))
		}
	}
	if len(realInfo.Channels) > 0 {
		if !reflect.DeepEqual(genInfo.Channels, realInfo.Channels) {
			return fmt.Errorf("Channels mismatch: %v != %v", genInfo.Channels, realInfo.Channels)
		}
	}
	if len(realInfo.Schemas) > 0 {
		if !reflect.DeepEqual(genInfo.Schemas, realInfo.Schemas) {
			return fmt.Errorf("Schemas mismatch: %v != %v", genInfo.Schemas, realInfo.Schemas)
		}
	}
	if len(realInfo.ChunkIndexes) > 0 {
		if !reflect.DeepEqual(genInfo.ChunkIndexes, realInfo.ChunkIndexes) {
			return fmt.Errorf("ChunkIndexes mismatch: %s != %s", dump(genInfo.ChunkIndexes), dump(realInfo.ChunkIndexes))
		}
	}
	if len(realInfo.MetadataIndexes) > 0 {
		if !reflect.DeepEqual(genInfo.MetadataIndexes, realInfo.MetadataIndexes) {
			return fmt.Errorf("MetadataIndexes mismatch: %s != %s", dump(genInfo.MetadataIndexes), dump(realInfo.MetadataIndexes))
		}
	}
	if len(realInfo.AttachmentIndexes) > 0 {
		if !reflect.DeepEqual(genInfo.AttachmentIndexes, realInfo.AttachmentIndexes) {
			return fmt.Errorf("AttachmentIndexes mismatch: %s != %s", dump(genInfo.AttachmentIndexes), dump(realInfo.AttachmentIndexes))
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
