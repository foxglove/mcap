package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/foxglove/mcap/go/libmcap"
	"github.com/spf13/cobra"
)

var infoCmd = &cobra.Command{
	Use:   "info",
	Short: "Report statistics about an mcap file",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			log.Fatal("Unexpected number of args")
		}
		r, err := os.Open(args[0])
		if err != nil {
			log.Fatal(err)
		}
		reader, err := libmcap.NewReader(r)
		if err != nil {
			log.Fatal(err)
		}
		info, err := reader.Info()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%+v\n", info)
	},
}

func init() {
	rootCmd.AddCommand(infoCmd)
}
