/// Utility for checking that the current git tag matches the internal MCAP library version string.
package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/foxglove/mcap/go/mcap"
)

func getGitTag() (string, error) {
	res, err := exec.Command("git", "describe", "--tags").Output()
	return string(res), err
}

func main() {
	tag, err := getGitTag()
	if err != nil {
		fmt.Printf("Error getting git tag: %e\n", err)
		os.Exit(1)
	}
	expected := fmt.Sprintf("go/mcap/%s", mcap.Version)
	if expected != tag {
		fmt.Println("Incorrect git tag for library version, expected", expected, "found", tag)
		os.Exit(1)
	}
}
