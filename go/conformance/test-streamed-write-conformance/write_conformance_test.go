package main

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriterConformance(t *testing.T) {
	inputs := []string{}
	err := filepath.Walk("../../../tests/conformance/data", func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".json" && !strings.Contains(path, "pad") {
			inputs = append(inputs, path)
		}
		return nil
	})
	assert.Nil(t, err)
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			output := bytes.Buffer{}
			err := jsonToMCAP(&output, input)
			assert.Nil(t, err)
			expectedBytes, err := os.ReadFile(strings.TrimSuffix(input, ".json") + ".mcap")
			assert.Nil(t, err)
			assert.Equal(t, expectedBytes, output.Bytes())
		})
	}
}
