package cmd

import (
	"bufio"
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/foxglove/go-rosbag"
	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/foxglove/mcap/go/ros"
	_ "github.com/mattn/go-sqlite3" // sqlite3 driver
	"github.com/spf13/cobra"
)

var (
	bagMagic = []byte("#ROSBAG V2.0")
	db3Magic = []byte{0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x20, 0x33, 0x00}
)

var (
	convertAmentPrefixPath string
	convertCompression     string
	convertChunkSize       int64
	convertIncludeCRC      bool
	convertChunked         bool
)

type FileType string

const (
	FileTypeRos1 FileType = "ros1"
	FileTypeDB3  FileType = "db3"
)

const (
	CompressionFormatLz4  = "lz4"
	CompressionFormatZstd = "zstd"
	CompressionFormatNone = "none"
)

func checkMagic(path string) (FileType, error) {
	f, err := os.Open(path)
	if err != nil {
		die("failed to open input: %s", err)
	}
	defer f.Close()

	magic := make([]byte, len(bagMagic))
	_, err = f.Read(magic)
	if err != nil {
		die("failed to read magic bytes: %s", err)
	}
	if bytes.Equal(magic, bagMagic) {
		return FileTypeRos1, nil
	}

	db3magic := make([]byte, len(db3Magic))
	n := copy(db3magic, magic)
	_, err = f.Read(db3magic[n:])
	if err != nil {
		die("failed to read magic bytes: %s", err)
	}
	if bytes.Equal(db3magic, db3Magic) {
		return FileTypeDB3, nil
	}
	return "", fmt.Errorf("unrecognized file type")
}

var convertCmd = &cobra.Command{
	Use:   "convert [input] [output]",
	Short: "Convert a bag file to an MCAP file",
	Args:  cobra.ExactArgs(2),
	Run: func(_ *cobra.Command, args []string) {
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

		var compressionFormat mcap.CompressionFormat
		switch convertCompression {
		case CompressionFormatLz4:
			compressionFormat = mcap.CompressionLZ4
		case CompressionFormatZstd:
			compressionFormat = mcap.CompressionZSTD
		case CompressionFormatNone:
			compressionFormat = mcap.CompressionNone
		}

		opts := &mcap.WriterOptions{
			IncludeCRC:  convertIncludeCRC,
			Chunked:     convertChunked,
			ChunkSize:   convertChunkSize,
			Compression: compressionFormat,
		}

		bw := bufio.NewWriter(w)
		defer bw.Flush()

		switch filetype {
		case FileTypeRos1:
			rosReader, err := rosbag.NewReader(f)
			if err != nil {
				die("failed to make new ROS reader: %s", err)
			}

			rosInfo, err := rosReader.Info()
			if err != nil {
				die("failed to get info from ROS file: %s", err)
			}

			messageCount := rosInfo.MessageCount
			progressBar := utils.NewProgressBar(int64(messageCount))
			_, err = f.Seek(0, io.SeekStart) // Info() moved the seeker to EOF. This brings it back to the start.
			if err != nil {
				die("failed to seek to start of file: %s", err)
			}

			err = ros.Bag2MCAP(bw, f, opts, func([]byte) error {
				progressBarErr := progressBar.Add64(1)
				if progressBarErr != nil {
					die("failed to increment progressbar: %s", err)
				}
				return nil
			})
			if err != nil && !errors.Is(err, io.EOF) {
				die("failed to convert file: %s", err)
			}
		case FileTypeDB3:
			f.Close()
			db, err := sql.Open("sqlite3", args[0])
			if err != nil {
				die("failed to open sqlite3: %s", err)
			}

			var messageCount int
			err = db.QueryRow("select count(*) from messages").Scan(&messageCount)
			if err != nil {
				die("failed to query db for message counts")
			}
			progressBar := utils.NewProgressBar(int64(messageCount))

			amentPath := convertAmentPrefixPath
			prefixPath := os.Getenv("AMENT_PREFIX_PATH")
			if prefixPath != "" {
				amentPath += string(os.PathListSeparator) + prefixPath
			}
			dirs := strings.FieldsFunc(amentPath, func(c rune) bool { return (c == os.PathListSeparator) })
			err = ros.DB3ToMCAP(bw, db, opts, dirs, func([]byte) error {
				progressBarErr := progressBar.Add64(1)
				if progressBarErr != nil {
					die("failed to increment progressbar: %s", err)
				}
				return nil
			})
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
		&convertAmentPrefixPath,
		"ament-prefix-path",
		"",
		"",
		"(ros2 only) list of directories to search for message definitions\n"+
			"separated by ':' on Unix-like platforms and ';' on Windows\n"+
			"(e.g /opt/ros/galactic:/opt/ros/noetic, or C:\\opt\\ros\\galactic;C:\\opt\\ros\\noetic)",
	)
	convertCmd.PersistentFlags().StringVarP(
		&convertCompression,
		"compression",
		"",
		"zstd",
		"chunk compression algorithm (supported: zstd, lz4, none)",
	)
	convertCmd.PersistentFlags().Int64VarP(
		&convertChunkSize,
		"chunk-size",
		"",
		8*1024*1024,
		"chunk size to target",
	)
	convertCmd.PersistentFlags().BoolVarP(
		&convertIncludeCRC,
		"include-crc",
		"",
		true,
		"include chunk CRC checksums in output",
	)
	convertCmd.PersistentFlags().BoolVarP(
		&convertChunked,
		"chunked",
		"",
		true,
		"chunk the output file",
	)
}
