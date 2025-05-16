package utils

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRebuild(t *testing.T) {
	err := filepath.Walk("../../../../tests/conformance/data/", func(path string, info os.FileInfo, err error) error {
		// skip unchunked files
		if !strings.Contains(path, "-ch") {
			return nil
		}
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".mcap") {
			t.Run(path, func(t *testing.T) {
				input, err := os.ReadFile(path)
				require.NoError(t, err)
				r := bytes.NewReader(input)

				_, err = RebuildInfo(r, false)
				require.NoError(t, err)
			})
		}
		return nil
	})
	require.NoError(t, err)
}
