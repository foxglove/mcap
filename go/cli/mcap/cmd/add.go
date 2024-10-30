package cmd

import "github.com/spf13/cobra"

var addCmd = &cobra.Command{
	Use:   "add",
	Short: "Add records to an existing MCAP file",
	Run: func(*cobra.Command, []string) {
	},
}

func init() {
	rootCmd.AddCommand(addCmd)
}
