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
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			output := bytes.Buffer{}
			err := readStreamed(&output, input)
			require.NoError(t, err)
			expectedBytes, err := os.ReadFile(strings.TrimSuffix(input, ".mcap") + ".json")
			require.NoError(t, err)
			expectedOutput := TextOutput{}
			err = json.Unmarshal(expectedBytes, &expectedOutput)
			require.NoError(t, err)
			receivedOutput := TextOutput{}
			err = json.Unmarshal(output.Bytes(), &receivedOutput)
			require.NoError(t, err)
			expectedRecords, err := json.Marshal(expectedOutput.Records)
			require.NoError(t, err)
			receivedRecords, err := json.Marshal(receivedOutput.Records)
			require.NoError(t, err)
			expectedPretty, err := prettifyJSON(expectedRecords)
			require.NoError(t, err)
			receivedPretty, err := prettifyJSON(receivedRecords)
			require.NoError(t, err)
			assert.Equal(t, string(expectedPretty), string(receivedPretty))
		})
	}
}
