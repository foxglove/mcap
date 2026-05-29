package cmd

import (
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
)

func TestVersionOutputIncludesCliAndLibraryVersions(t *testing.T) {
	oldCliVersion := Version
	oldLibraryVersion := mcap.Version
	t.Cleanup(func() {
		Version = oldCliVersion
		mcap.Version = oldLibraryVersion
	})

	Version = "cli-test-version"
	mcap.Version = "library-test-version"

	assert.Equal(t, "mcap cli version: cli-test-version\nmcap library version: library-test-version\n", versionOutput())
}
