package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var (
	addAttachmentLogTime      uint64
	addAttachmentCreationTime uint64
	addAttachmentFilename     string
	addAttachmentMediaType    string
)

var (
	getAttachmentName   string
	getAttachmentOffset uint64
	getAttachmentOutput string
)

func getAttachment(w io.Writer, rs io.ReadSeeker, idx *mcap.AttachmentIndex) error {
	_, err := rs.Seek(int64(
		idx.Offset+
			1+ // opcode
			8+ // record length
			8+ // log time
			8+ // creation time
			4+ // name length
			uint64(len(idx.Name))+
			4+ // content type length
			uint64(len(idx.MediaType))+
			8), // data length
		io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to offset %d: %w", idx.Offset, err)
	}
	_, err = io.CopyN(w, rs, int64(idx.DataSize))
	if err != nil {
		return fmt.Errorf("failed to copy attachment to output: %w", err)
	}
	return nil
}

var getAttachmentCmd = &cobra.Command{
	Use:   "attachment",
	Short: "Get an attachment by name or offset",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		if len(args) != 1 {
			die("Unexpected number of args")
		}
		filename := args[0]

		var output io.Writer
		var err error
		if getAttachmentOutput == "" {
			if !utils.StdoutRedirected() {
				die(PleaseRedirect)
			}
			output = os.Stdout
		} else {
			output, err = os.Create(getAttachmentOutput)
			if err != nil {
				die("failed to create output file: %s", err)
			}
		}

		err = utils.WithReader(ctx, filename, func(_ bool, rs io.ReadSeeker) error {
			reader, err := mcap.NewReader(rs)
			if err != nil {
				return fmt.Errorf("failed to construct reader: %w", err)
			}
			info, err := reader.Info()
			if err != nil {
				return fmt.Errorf("failed to get mcap info: %w", err)
			}
			attachments := make(map[string][]*mcap.AttachmentIndex)
			for _, attachmentIdx := range info.AttachmentIndexes {
				attachments[attachmentIdx.Name] = append(
					attachments[attachmentIdx.Name],
					attachmentIdx,
				)
			}

			switch {
			case len(attachments[getAttachmentName]) == 0:
				die("attachment %s not found", getAttachmentName)
			case len(attachments[getAttachmentName]) == 1:
				getAttachment(output, rs, attachments[getAttachmentName][0])
			case len(attachments[getAttachmentName]) > 1:
				if getAttachmentOffset == 0 {
					return fmt.Errorf(
						"multiple attachments named %s exist. Specify an offset.",
						getAttachmentName,
					)
				}
				for _, idx := range attachments[getAttachmentName] {
					if idx.Offset == getAttachmentOffset {
						return getAttachment(output, rs, idx)
					}
				}
				return fmt.Errorf(
					"failed to find attachment %s at offset %d",
					getAttachmentName,
					getAttachmentOffset,
				)
			}
			return nil
		})
		if err != nil {
			die("failed to extract attachment: %s", err)
		}
	},
}

var addAttachmentCmd = &cobra.Command{
	Use:   "attachment",
	Short: "Add an attachment to an MCAP file",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		if len(args) != 1 {
			die("Unexpected number of args")
		}
		filename := args[0]
		tempName := filename + ".new"
		tmpfile, err := os.Create(tempName)
		if err != nil {
			die("failed to create temp file: %s", err)
		}
		attachment, err := os.ReadFile(addAttachmentFilename)
		if err != nil {
			die("failed to read attachment: %s", err)
		}
		err = utils.WithReader(ctx, filename, func(remote bool, rs io.ReadSeeker) error {
			if remote {
				die("not supported on remote MCAP files")
			}
			fi, err := os.Stat(addAttachmentFilename)
			if err != nil {
				die("failed to stat file %s", addAttachmentFilename)
			}
			createTime := uint64(fi.ModTime().UTC().UnixNano())
			if addAttachmentCreationTime > 0 {
				createTime = addAttachmentCreationTime
			}
			logTime := uint64(time.Now().UTC().UnixNano())
			if addAttachmentLogTime > 0 {
				logTime = addAttachmentLogTime
			}
			return utils.RewriteMCAP(tmpfile, rs, func(w *mcap.Writer) error {
				return w.WriteAttachment(&mcap.Attachment{
					LogTime:    logTime,
					CreateTime: createTime,
					Name:       addAttachmentFilename,
					MediaType:  addAttachmentMediaType,
					Data:       attachment,
				})
			})
		})
		if err != nil {
			die("failed to add attachment: %s", err)
		}
		err = os.Rename(tempName, filename)
		if err != nil {
			die("failed to rename temporary output: %s", err)
		}
	},
}

func init() {
	addCmd.AddCommand(addAttachmentCmd)
	addAttachmentCmd.PersistentFlags().StringVarP(&addAttachmentFilename, "file", "f", "", "filename of attachment to add")
	addAttachmentCmd.PersistentFlags().StringVarP(&addAttachmentMediaType, "content-type", "", "application/octet-stream", "content type of attachment")
	addAttachmentCmd.PersistentFlags().Uint64VarP(&addAttachmentLogTime, "log-time", "", 0, "attachment log time in nanoseconds (defaults to current timestamp)")
	addAttachmentCmd.PersistentFlags().Uint64VarP(&addAttachmentLogTime, "creation-time", "", 0, "attachment creation time in nanoseconds (defaults to ctime)")
	addAttachmentCmd.MarkPersistentFlagRequired("file")

	getCmd.AddCommand(getAttachmentCmd)
	getAttachmentCmd.PersistentFlags().StringVarP(&getAttachmentName, "name", "n", "", "name of attachment to extract")
	getAttachmentCmd.PersistentFlags().Uint64VarP(&getAttachmentOffset, "offset", "", 0, "offset of attachment to extract")
	getAttachmentCmd.PersistentFlags().StringVarP(&getAttachmentOutput, "output", "o", "", "location to write attachment to")
	getAttachmentCmd.MarkPersistentFlagRequired("name")
}
