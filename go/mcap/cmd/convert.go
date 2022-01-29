package cmd

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/foxglove/mcap/go/ros"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/cobra"
)

var (
	rosMagic = []byte("ROSBAG V2.0\n")
	db3Magic = []byte{0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x20, 0x33, 0x00}
)

var directories string

func checkMagic(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	rosmagic := make([]byte, len(rosMagic))
	_, err = f.Read(rosmagic)
	if err != nil {
		log.Fatal(err)
	}
	if bytes.Equal(rosmagic, rosMagic) {
		return "ros1", nil
	}

	db3magic := make([]byte, len(db3Magic))
	n := copy(db3magic, rosmagic)
	_, err = f.Read(db3magic[n:])
	if err != nil {
		log.Fatal(err)
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
			log.Fatalf("Magic number check failed: %s", err)
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

		switch filetype {
		case "ros1":
			err = ros.Bag2MCAP(f, w)
			if err != nil && !errors.Is(err, io.EOF) {
				log.Fatal("failed to convert file: ", err)
			}
		case "db3":
			f.Close()
			db, err := sql.Open("sqlite3", args[0])
			if err != nil {
				log.Fatal("failed to open sqlite3: %w", err)
			}
			dirs := strings.FieldsFunc(directories, func(c rune) bool { return c == ',' })
			prefix := os.Getenv("AMENT_PREFIX_PATH")
			if prefix != "" {
				dirs = append(dirs, prefix)
			}
			err = ros.DB3ToMCAP(db, w, dirs)
			if err != nil {
				log.Fatal("failed to convert file: ", err)
			}
		default:
			log.Fatalf("unsupported format: %s", filetype)
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
