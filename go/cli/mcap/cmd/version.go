package cmd

import (
	"fmt"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var Version = "(devel)"

// versionCmd represents the version command.
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Output version information",
	Run: func(*cobra.Command, []string) {
		fmt.Print(versionOutput())
	},
}

func versionOutput() string {
	return fmt.Sprintf("mcap cli version: %s\nmcap library version: %s\n", Version, mcap.Version)
}

func configureVersionOutput() {
	rootCmd.Version = Version
	rootCmd.SetVersionTemplate(versionOutput())
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
