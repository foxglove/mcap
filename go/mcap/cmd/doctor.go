package cmd

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/foxglove/mcap/go/libmcap"
	"github.com/spf13/cobra"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor [file]",
	Short: "Validate that a file is a valid MCAP file",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filename := args[0]
		f, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		// lex the file twice - once with dechunking, and once without
		for _, emitChunks := range []bool{true, false} {
			_, err := f.Seek(0, os.SEEK_SET)
			if err != nil {
				log.Fatal("Failed to seek to start of file: ", err)
			}
			lexer, err := libmcap.NewLexer(f, &libmcap.LexOpts{
				SkipMagic:   false,
				ValidateCRC: true,
				EmitChunks:  emitChunks,
			})
			if err != nil {
				log.Fatal(err)
			}
			msg := make([]byte, 1024)
			for {
				tokenType, data, err := lexer.Next(msg)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					log.Fatal("Failed to read token:", err)
				}
				if len(data) > len(msg) {
					msg = data
				}
				switch tokenType {
				case libmcap.TokenHeader:
					_, err := libmcap.ParseHeader(data)
					if err != nil {
						fmt.Println("Failed to parse header:", err)
					}
				case libmcap.TokenFooter:
					_, err := libmcap.ParseFooter(data)
					if err != nil {
						fmt.Println("Failed to parse footer:", err)
					}
				case libmcap.TokenSchema:
					_, err := libmcap.ParseSchema(data)
					if err != nil {
						fmt.Println("Failed to parse schema:", err)
					}
				case libmcap.TokenChannel:
					_, err := libmcap.ParseChannel(data)
					if err != nil {
						fmt.Println("Failed to parse channel:", err)
					}
				case libmcap.TokenMessage:
					_, err := libmcap.ParseMessage(data)
					if err != nil {
						fmt.Println("Failed to parse message:", err)
					}
				case libmcap.TokenChunk:
					_, err := libmcap.ParseChunk(data)
					if err != nil {
						fmt.Println("Failed to parse chunk:", err)
					}
				case libmcap.TokenMessageIndex:
					_, err := libmcap.ParseMessageIndex(data)
					if err != nil {
						fmt.Println("Failed to parse message index:", err)
					}
				case libmcap.TokenChunkIndex:
					_, err := libmcap.ParseChunkIndex(data)
					if err != nil {
						fmt.Println("Failed to parse chunk index:", err)
					}
				case libmcap.TokenAttachment:
					_, err := libmcap.ParseAttachment(data)
					if err != nil {
						fmt.Println("Failed to parse attachment:", err)
					}
				case libmcap.TokenAttachmentIndex:
					_, err := libmcap.ParseAttachmentIndex(data)
					if err != nil {
						fmt.Println("Failed to parse attachment index:", err)
					}
				case libmcap.TokenStatistics:
					_, err := libmcap.ParseStatistics(data)
					if err != nil {
						fmt.Println("Failed to parse statistics:", err)
					}
				case libmcap.TokenMetadata:
					_, err := libmcap.ParseMetadata(data)
					if err != nil {
						fmt.Println("Failed to parse metadata:", err)
					}
				case libmcap.TokenMetadataIndex:
					_, err := libmcap.ParseMetadataIndex(data)
					if err != nil {
						fmt.Println("Failed to parse metadata index:", err)
					}
				case libmcap.TokenSummaryOffset:
					_, err := libmcap.ParseSummaryOffset(data)
					if err != nil {
						fmt.Println("Failed to parse summary offset:", err)
					}
				case libmcap.TokenDataEnd:
					_, err := libmcap.ParseDataEnd(data)
					if err != nil {
						fmt.Println("Failed to parse data end:", err)
					}
				case libmcap.TokenError:
					// this is the value of the tokenType when there is an error
					// from the lexer, which we caught at the top.
					log.Fatal("Failed to lex file:", err)
				}
			}
		}
		fmt.Println("File is valid")
	},
}

func init() {
	rootCmd.AddCommand(doctorCmd)

}
