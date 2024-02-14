package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
)

func TestInfo(t *testing.T) {
	err := filepath.Walk("../../../../tests/conformance/data/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".mcap") {
			t.Run(path, func(t *testing.T) {
				input, err := os.ReadFile(path)
				assert.Nil(t, err)
				r := bytes.NewReader(input)
				w := new(bytes.Buffer)

				reader, err := mcap.NewReader(r)
				assert.Nil(t, err)
				defer reader.Close()
				info, err := reader.Info()
				assert.Nil(t, err)
				err = printInfo(w, info)
				assert.Nil(t, err)
			})
		}
		return nil
	})
	assert.Nil(t, err)
}

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		n      uint64
		result string
	}{
		{2, "2.00 B"},
		{1024 * 2, "2.00 KiB"},
		{1024 * 1024 * 2, "2.00 MiB"},
		{1024 * 1024 * 1024 * 2, "2.00 GiB"},
		{1024 * 1024 * 1024 * 1024 * 2, "2048.00 GiB"},
	}
	for _, c := range cases {
		t.Run(c.result, func(t *testing.T) {
			assert.Equal(t, c.result, humanBytes(c.n))
		})
	}
}
