package cmd

import (
	"io"
	"os"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

func recoverInPlace(file *os.File) error {
	rebuildData, err := utils.RebuildInfo(file)
	if err != nil {
		return err
	}

	fileSize, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	_, err = file.Seek(rebuildData.DataEndOffset, io.SeekStart)
	if err != nil {
		return err
	}

	writer, err := mcap.NewWriter(file, &mcap.WriterOptions{
		SkipMagic: true,
	})
	if err != nil {
		return err
	}

	err = writer.WriteDataEnd(&mcap.DataEnd{
		DataSectionCRC: rebuildData.DataSectionCRC,
	})
	if err != nil {
		return err
	}
	err = utils.WriteInfo(file, rebuildData.Info)
	if err != nil {
		return err
	}

	currentOffset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if currentOffset < fileSize {
		// Truncate the file to the current offset
		err = file.Truncate(currentOffset)
		if err != nil {
			return err
		}
	}
	return nil
}

func init() {
	var recoverInPlaceCmd = &cobra.Command{
		Use:   "recover-in-place [file]",
		Short: "Recover data from a potentially corrupt MCAP file",
		Long: `This subcommand reads a potentially corrupt MCAP file and fixes it in place.

usage:
  mcap recover-in-place in.mcap`,
	}

	recoverInPlaceCmd.Run = func(_ *cobra.Command, args []string) {
		if len(args) == 0 {
			die("please supply a file. see --help for usage details.")
		} else {
			file, err := os.OpenFile(args[0], os.O_RDWR, 0)
			if err != nil {
				die("failed to open file: %s", err)
			}
			defer file.Close()
			err = recoverInPlace(file)
			if err != nil {
				die("failed to recover file: %s", err)
			}
		}
	}
	rootCmd.AddCommand(recoverInPlaceCmd)
}
