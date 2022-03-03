package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var Version string

// versionCmd represents the version command
func versionCmd(version string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Output version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(Version)
		},
	}
}

func init() {
	rootCmd.AddCommand(versionCmd(Version))
}
