package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getLogTime(fields []any) uint64 {
	for _, field := range fields {
		tup, ok := field.([]any)
		if !ok {
			panic(fmt.Sprintf("field tuple is: %v", field))
		}
		if tup[0] == "log_time" {
			val, err := strconv.ParseUint(tup[1].(string), 10, 64)
			if err != nil {
				panic(err.Error())
			}
			return val
		}
	}
	panic(fmt.Sprintf("where's the log_time in %v", fields))
}

func sortMessages(output TextOutput) {
	//hack: assumes messages are contiguous within a file
	firstMessageIdx := 0
	lastMessageIdx := 0
	for i := 0; i < len(output.Records); i++ {
		if output.Records[i].Type == "Message" && firstMessageIdx == 0 {
			firstMessageIdx = i
		}
		if firstMessageIdx != 0 && output.Records[i].Type != "Message" {
			lastMessageIdx = i
		}
		if firstMessageIdx != 0 && lastMessageIdx != 0 {
			break
		}
	}
	messages := output.Records[firstMessageIdx:lastMessageIdx]
	sort.Slice(messages, func(i, j int) bool {
		logTimeI := getLogTime(messages[i].Fields)
		logTimeJ := getLogTime(messages[j].Fields)
		return logTimeI < logTimeJ
	})
}

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
			if errors.Is(err, mcap.ErrInsufficientSummary) {
				return
			}
			require.NoError(t, err)
			expectedBytes, err := os.ReadFile(strings.TrimSuffix(input, ".mcap") + ".json")
			assert.Nil(t, err)
			expectedOutput := TextOutput{}
			err = json.Unmarshal(expectedBytes, &expectedOutput)
			assert.Nil(t, err)
			receivedOutput := TextOutput{}
			sortMessages(expectedOutput)
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
