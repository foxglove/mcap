package cmd

import (
	"errors"
	"io"
	"log"
	"os"

	"github.com/foxglove/mcap/go/libmcap"
	"github.com/spf13/cobra"
)

var convertCmd = &cobra.Command{
	Use:   "convert [input] [output]",
	Short: "Convert a bag file to an mcap file",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 2 {
			log.Fatal("supply an input and output file (see mcap convert -h)")
		}
		f, err := os.Open(args[0])
		if err != nil {
			log.Fatal("failed to open input: %w", err)
		}
		defer f.Close()
		w, err := os.Create(args[1])
		if err != nil {
			log.Fatal("failed to open output: %w", err)
		}
		defer w.Close()
		err = libmcap.Bag2MCAP(f, w)
		if err != nil && !errors.Is(err, io.EOF) {
			log.Fatal("failed to convert file: ", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(convertCmd)
}
