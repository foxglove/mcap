package cmd

import (
	"github.com/spf13/cobra"
)

var renameCmd = &cobra.Command{
	Use:   "rename",
	Short: "Rename records of an MCAP file",
	Run: func(cmd *cobra.Command, _ []string) {
		err := cmd.Help()
		if err != nil {
			die("failed to run help command: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(renameCmd)
}
