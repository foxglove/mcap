/// Utility for checking that the current git tag matches the internal MCAP library version string.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/foxglove/mcap/go/mcap"
)

func getGitTags() ([]string, error) {
	res, err := exec.Command("git", "tag", "--points-at", "HEAD").Output()
	return strings.Split(string(res), "\n"), err
}

func main() {
	tags, err := getGitTags()
	if err != nil {
		fmt.Printf("Error getting git tags: %e\n", err)
		os.Exit(1)
	}
	expected := fmt.Sprintf("go/mcap/%s", mcap.Version)
	found := false
	for _, tag := range tags {
		if expected == tag {
			found = true
			break
		}
	}
	if !found {
		fmt.Println("Did not find git tag for library version, expected", expected, "found", tags)
		os.Exit(1)
	} else {
		fmt.Println("Success")
	}
}
