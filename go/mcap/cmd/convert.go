package cmd

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/foxglove/mcap/go/ros"
	_ "github.com/mattn/go-sqlite3" // sqlite3 driver
	"github.com/spf13/cobra"
)

var (
	rosMagic = []byte("#ROSBAG V2.0")
	db3Magic = []byte{0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x20, 0x33, 0x00}
)

var directories string

func checkMagic(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		die("failed to open input: %s", err)
	}
	defer f.Close()

	rosmagic := make([]byte, len(rosMagic))
	_, err = f.Read(rosmagic)
	if err != nil {
		die("failed to read magic bytes: %s", err)
	}
	if bytes.Equal(rosmagic, rosMagic) {
		return "ros1", nil
	}

	db3magic := make([]byte, len(db3Magic))
	n := copy(db3magic, rosmagic)
	_, err = f.Read(db3magic[n:])
	if err != nil {
		die("failed to read magic bytes: %s", err)
	}
	if bytes.Equal(db3magic, db3Magic) {
		return "db3", nil
	}
	return "", fmt.Errorf("unrecognized file type")
}

var convertCmd = &cobra.Command{
	Use:   "convert [input] [output]",
	Short: "Convert a bag file to an mcap file",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		filetype, err := checkMagic(args[0])
		if err != nil {
			die("magic number check failed: %s", err)
		}

		f, err := os.Open(args[0])
		if err != nil {
			die("failed to open input: %s", err)
		}
		defer f.Close()
		w, err := os.Create(args[1])
		if err != nil {
			die("failed to open output: %s", err)
		}
		defer w.Close()

		switch filetype {
		case "ros1":
			err = ros.Bag2MCAP(w, f)
			if err != nil && !errors.Is(err, io.EOF) {
				die("failed to convert file: %s", err)
			}
		case "db3":
			f.Close()
			db, err := sql.Open("sqlite3", args[0])
			if err != nil {
				die("failed to open sqlite3: %s", err)
			}
			dirs := strings.FieldsFunc(directories, func(c rune) bool { return c == ',' })
			prefix := os.Getenv("AMENT_PREFIX_PATH")
			if prefix != "" {
				dirs = append(dirs, prefix)
			}
			err = ros.DB3ToMCAP(w, db, dirs)
			if err != nil {
				die("failed to convert file: %s", err)
			}
		default:
			die("unsupported format: %s", filetype)
		}
	},
}

func init() {
	rootCmd.AddCommand(convertCmd)
	convertCmd.PersistentFlags().StringVarP(
		&directories,
		"directories",
		"",
		"",
		"comma-separated list of directories to search for messages, e.g /opt/ros/galactic",
	)
}
