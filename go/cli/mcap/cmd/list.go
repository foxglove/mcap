package cmd

import (
	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List records of an MCAP file",
	Run: func(cmd *cobra.Command, args []string) {
		err := cmd.Help()
		if err != nil {
			die("failed to run help command: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(listCmd)
}
