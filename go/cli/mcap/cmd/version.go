package cmd

import (
	"fmt"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var Version string
var printLibraryVersion bool

// versionCmd represents the version command.
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Output version information",
	Run: func(*cobra.Command, []string) {
		if printLibraryVersion {
			fmt.Println(mcap.Version)
		} else {
			fmt.Println(Version)
		}
	},
}

func init() {
	versionCmd.PersistentFlags().BoolVarP(
		&printLibraryVersion,
		"library",
		"l",
		false,
		"print MCAP library version instead of CLI version",
	)
	rootCmd.AddCommand(versionCmd)
}
