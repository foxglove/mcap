package main

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLexerConformance(t *testing.T) {
	inputs := []string{}
	err := filepath.Walk("../../../tests/conformance/data", func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".mcap" {
			inputs = append(inputs, path)
		}
		return nil
	})
	assert.Nil(t, err)
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			output := bytes.Buffer{}
			err := readStreamed(&output, input)
			assert.Nil(t, err)
			expectedBytes, err := os.ReadFile(strings.TrimSuffix(input, ".mcap") + ".json")
			assert.Nil(t, err)
			expectedOutput := TextOutput{}
			err = json.Unmarshal(expectedBytes, &expectedOutput)
			assert.Nil(t, err)
			receivedOutput := TextOutput{}
			err = json.Unmarshal(output.Bytes(), &receivedOutput)
			assert.Nil(t, err)
			expectedRecords, err := json.Marshal(expectedOutput.Records)
			assert.Nil(t, err)
			receivedRecords, err := json.Marshal(receivedOutput.Records)
			assert.Nil(t, err)
			expectedPretty, err := prettifyJSON(expectedRecords)
			assert.Nil(t, err)
			receivedPretty, err := prettifyJSON(receivedRecords)
			assert.Nil(t, err)
			assert.Equal(t, string(expectedPretty), string(receivedPretty))
		})
	}
}
