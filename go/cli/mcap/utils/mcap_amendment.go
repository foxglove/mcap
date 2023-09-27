package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/foxglove/mcap/go/mcap"
)

const (
	SizeRecordLength = 8
	SizeOpcode       = 1
	SizeDataEnd      = 4
	SizeFooter       = 8 + 8 + 4
	SizeMagic        = 8
)

// AmendMCAP adds attachment and metadata records to the end of the
// data section of an existing MCAP, in place. It then writes a new summary
// section consisting of the existing summary section, plus new attachment and
// metadata index records as applicable, with all offsets and CRCs updated to
// account for the new data.
func AmendMCAP(
	wsc io.ReadWriteSeeker,
	attachments []*mcap.Attachment,
	metadata []*mcap.Metadata,
) error {
	lexer, err := mcap.NewLexer(wsc, &mcap.LexerOptions{
		SkipMagic: true,
	})
	if err != nil {
		return fmt.Errorf("failed to build lexer: %w", err)
	}
	defer lexer.Close()

	oldFooter, err := readFooter(wsc)
	if err != nil {
		return fmt.Errorf("failed to read footer: %w", err)
	}
	oldSummaryStart := oldFooter.SummaryStart

	// seek to the opcode of the data end record
	oldDataEndOffset := int64(oldSummaryStart - SizeDataEnd - SizeRecordLength - SizeOpcode)
	oldDataEnd, err := readDataEnd(wsc, oldDataEndOffset)
	if err != nil {
		return fmt.Errorf("failed to read data end: %w", err)
	}

	// Now we are at the old summary start. Parse from here to EOF into a
	// summarySection structure.
	summary, err := readSummarySection(wsc)
	if err != nil {
		return fmt.Errorf("failed to read summary section: %w", err)
	}

	// Seek to the location of the old data end record. We will write the new data section records starting here.
	_, err = wsc.Seek(oldDataEndOffset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to data end: %w", err)
	}

	extraDataLength, attachmentIndexes, metadataIndexes, err := extendDataSection(
		wsc,
		oldDataEndOffset,
		oldDataEnd.DataSectionCRC,
		attachments,
		metadata,
	)
	if err != nil {
		return fmt.Errorf("failed to build extra data: %w", err)
	}

	// the new summary offset is the old data end offset plus the length just written.
	newSummaryOffset := oldDataEndOffset + extraDataLength

	// Update summary with the new attachment and metadata indexes.
	for _, attachmentIndex := range attachmentIndexes {
		summary.AttachmentIndexes = append(summary.AttachmentIndexes, attachmentIndex)
		summary.Statistics.AttachmentCount++
	}
	for _, metadataIndex := range metadataIndexes {
		summary.MetadataIndexes = append(summary.MetadataIndexes, metadataIndex)
		summary.Statistics.MetadataCount++
	}

	// Write summary out into a well-formed summary section, with all offsets and CRCs updated.
	err = writeSummaryBytes(wsc, summary, newSummaryOffset)
	if err != nil {
		return fmt.Errorf("failed to build new summary section: %w", err)
	}

	return nil
}

// summarySection is a structure that contains all the records in the summary
// section of an MCAP.
type summarySection struct {
	Channels          []*mcap.Channel
	Schemas           []*mcap.Schema
	AttachmentIndexes []*mcap.AttachmentIndex
	MetadataIndexes   []*mcap.MetadataIndex
	ChunkIndexes      []*mcap.ChunkIndex
	SummaryOffsets    []*mcap.SummaryOffset
	Statistics        *mcap.Statistics
	Footer            *mcap.Footer
}

// readSummarySection reads MCAP data into a summary section structure. The
// reader should be positioned at the start of the summary section, and must be
// positioned at the start of a record.
func readSummarySection(r io.Reader) (*summarySection, error) {
	lexer, err := mcap.NewLexer(r, &mcap.LexerOptions{
		SkipMagic: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to construct summary section lexer: %w", err)
	}
	result := summarySection{}
	for {
		tokenType, token, err := lexer.Next(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to pull next record: %w", err)
		}
		switch tokenType {
		case mcap.TokenChannel:
			record, err := mcap.ParseChannel(token)
			if err != nil {
				return nil, fmt.Errorf("failed to parse channel: %w", err)
			}
			result.Channels = append(result.Channels, record)
		case mcap.TokenSchema:
			record, err := mcap.ParseSchema(token)
			if err != nil {
				return nil, fmt.Errorf("failed to parse schema: %w", err)
			}
			result.Schemas = append(result.Schemas, record)
		case mcap.TokenChunkIndex:
			record, err := mcap.ParseChunkIndex(token)
			if err != nil {
				return nil, fmt.Errorf("failed to parse chunk index: %w", err)
			}
			result.ChunkIndexes = append(result.ChunkIndexes, record)
		case mcap.TokenAttachmentIndex:
			record, err := mcap.ParseAttachmentIndex(token)
			if err != nil {
				return nil, fmt.Errorf("failed to parse attachment index: %w", err)
			}
			result.AttachmentIndexes = append(result.AttachmentIndexes, record)
		case mcap.TokenMetadataIndex:
			record, err := mcap.ParseMetadataIndex(token)
			if err != nil {
				return nil, fmt.Errorf("failed to parse metadata index: %w", err)
			}
			result.MetadataIndexes = append(result.MetadataIndexes, record)
		case mcap.TokenSummaryOffset:
			record, err := mcap.ParseSummaryOffset(token)
			if err != nil {
				return nil, fmt.Errorf("failed to parse summary offset: %w", err)
			}
			result.SummaryOffsets = append(result.SummaryOffsets, record)
		case mcap.TokenStatistics:
			record, err := mcap.ParseStatistics(token)
			if err != nil {
				return nil, fmt.Errorf("failed to parse statistics: %w", err)
			}
			result.Statistics = record
		case mcap.TokenFooter:
			record, err := mcap.ParseFooter(token)
			if err != nil {
				return nil, fmt.Errorf("failed to parse footer: %w", err)
			}
			result.Footer = record
			return &result, nil
		}
	}
}

// buildSummaryBytes constructs a summary byte section from a summarySection
// structure, including the footer and closing magic, given a start offset. The
// sections/offset groups may be rewritten in a different order than the in the
// data the summarySection was originally parsed from. All offsets and CRCs are
// updated to account.
func writeSummaryBytes(w io.Writer, section *summarySection, summaryStart int64) error {
	wc := newChecksummingWriteCounter(w, 0)
	writer, err := mcap.NewWriter(wc, &mcap.WriterOptions{
		SkipMagic: true,
	})
	if err != nil {
		return fmt.Errorf("failed to construct summary section writer: %w", err)
	}

	fileOffset := summaryStart
	summaryInset := int64(0)
	summaryOffsets := []*mcap.SummaryOffset{}

	for _, schema := range section.Schemas {
		err := writer.WriteSchema(schema)
		if err != nil {
			return fmt.Errorf("failed to write schema: %w", err)
		}
	}
	summaryOffsets = append(summaryOffsets, &mcap.SummaryOffset{
		GroupOpcode: mcap.OpSchema,
		GroupStart:  uint64(fileOffset),
		GroupLength: uint64(wc.Count()) - uint64(summaryInset),
	})

	fileOffset += wc.Count() - summaryInset
	summaryInset = wc.Count()

	for _, channel := range section.Channels {
		err := writer.WriteChannel(channel)
		if err != nil {
			return fmt.Errorf("failed to write channel: %w", err)
		}
	}
	summaryOffsets = append(summaryOffsets, &mcap.SummaryOffset{
		GroupOpcode: mcap.OpChannel,
		GroupStart:  uint64(fileOffset),
		GroupLength: uint64(wc.Count()) - uint64(summaryInset),
	})
	fileOffset += wc.Count() - summaryInset
	summaryInset = wc.Count()

	for _, attachmentIndex := range section.AttachmentIndexes {
		err := writer.WriteAttachmentIndex(attachmentIndex)
		if err != nil {
			return fmt.Errorf("failed to write attachment index: %w", err)
		}
	}
	summaryOffsets = append(summaryOffsets, &mcap.SummaryOffset{
		GroupOpcode: mcap.OpAttachmentIndex,
		GroupStart:  uint64(fileOffset),
		GroupLength: uint64(wc.Count()) - uint64(summaryInset),
	})

	fileOffset += wc.Count() - summaryInset
	summaryInset = wc.Count()

	for _, metadataIndex := range section.MetadataIndexes {
		err := writer.WriteMetadataIndex(metadataIndex)
		if err != nil {
			return fmt.Errorf("failed to write metadata index: %w", err)
		}
	}
	summaryOffsets = append(summaryOffsets, &mcap.SummaryOffset{
		GroupOpcode: mcap.OpMetadataIndex,
		GroupStart:  uint64(fileOffset),
		GroupLength: uint64(wc.Count()) - uint64(summaryInset),
	})

	fileOffset += wc.Count() - summaryInset
	summaryInset = wc.Count()

	for _, chunkIndex := range section.ChunkIndexes {
		err := writer.WriteChunkIndex(chunkIndex)
		if err != nil {
			return fmt.Errorf("failed to write chunk index: %w", err)
		}
	}
	summaryOffsets = append(summaryOffsets, &mcap.SummaryOffset{
		GroupOpcode: mcap.OpChunkIndex,
		GroupStart:  uint64(fileOffset),
		GroupLength: uint64(wc.Count()) - uint64(summaryInset),
	})

	fileOffset += wc.Count() - summaryInset
	summaryInset = wc.Count()

	err = writer.WriteStatistics(section.Statistics)
	if err != nil {
		return fmt.Errorf("failed to write statistics: %w", err)
	}

	summaryOffsets = append(summaryOffsets, &mcap.SummaryOffset{
		GroupOpcode: mcap.OpStatistics,
		GroupStart:  uint64(fileOffset),
		GroupLength: uint64(wc.Count()) - uint64(summaryInset),
	})

	fileOffset += wc.Count() - summaryInset
	summaryInset = wc.Count()
	summaryOffsetStart := fileOffset

	for _, summaryOffset := range summaryOffsets {
		err := writer.WriteSummaryOffset(summaryOffset)
		if err != nil {
			return fmt.Errorf("failed to write summary offset: %w", err)
		}
	}

	// Only compute a CRC if the existing CRC is nonzero. Interpret zero to mean
	// CRCs disabled.
	var summaryCRC uint32
	if section.Footer.SummaryCRC != 0 {
		summaryCRC = wc.CRC()
	}

	footer := &mcap.Footer{
		SummaryStart:       uint64(summaryStart),
		SummaryOffsetStart: uint64(summaryOffsetStart),
		SummaryCRC:         summaryCRC,
	}
	err = writeFooter(wc, footer)
	if err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}
	_, err = wc.Write(mcap.Magic)
	if err != nil {
		return fmt.Errorf("failed to write closing magic: %w", err)
	}
	return nil
}

// writeFooter writes a footer record onto a writer, updating the attached CRC
// to include the initial bytes.
func writeFooter(w io.Writer, footer *mcap.Footer) error {
	buf := make([]byte, 29)
	buf[0] = byte(mcap.OpFooter)
	inset := 1
	binary.LittleEndian.PutUint64(buf[inset:], 20)
	inset += 8
	binary.LittleEndian.PutUint64(buf[inset:], footer.SummaryStart)
	inset += 8
	binary.LittleEndian.PutUint64(buf[inset:], footer.SummaryOffsetStart)
	inset += 8
	var crc uint32
	if footer.SummaryCRC > 0 {
		crc = crc32.Update(footer.SummaryCRC, crc32.IEEETable, buf[:inset])
	}
	binary.LittleEndian.PutUint32(buf[inset:], crc)
	inset += 4
	if _, err := w.Write(buf[:inset]); err != nil {
		return err
	}
	return nil
}

// readFooter reads the footer record from the end of an MCAP.
func readFooter(rs io.ReadSeeker) (*mcap.Footer, error) {
	_, err := rs.Seek(-SizeMagic-SizeFooter, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to footer: %w", err)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to construct lexer: %w", err)
	}
	buf := make([]byte, 28)
	_, err = io.ReadFull(rs, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}
	if !bytes.Equal(buf[20:], mcap.Magic) {
		return nil, fmt.Errorf("invalid magic")
	}
	footer, err := mcap.ParseFooter(buf[:20])
	if err != nil {
		return nil, fmt.Errorf("failed to parse footer: %w", err)
	}
	return footer, nil
}

// readDataEnd reads a data end record at the supplied offset.
func readDataEnd(rs io.ReadSeeker, offset int64) (*mcap.DataEnd, error) {
	_, err := rs.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to data end: %w", err)
	}
	buf := make([]byte, SizeOpcode+SizeRecordLength+SizeDataEnd)
	_, err = io.ReadFull(rs, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read data end: %w", err)
	}
	if buf[0] != byte(mcap.OpDataEnd) {
		return nil, fmt.Errorf("expected data end opcode but got %d", mcap.OpCode(buf[0]))
	}
	dataEnd, err := mcap.ParseDataEnd(buf[SizeOpcode+SizeRecordLength:])
	if err != nil {
		return nil, fmt.Errorf("failed to parse data end: %w", err)
	}
	return dataEnd, nil
}

// extendDataSection writes attachments and metadata records over an existing
// DataEnd record, and then writes its own DataEnd at the end. It returns slices
// of attachment and metadata indexes. The startOffset supplied is used to
// compute offsets in these indexes, and startCRC - expected to be the CRC of
// the existing data section - is updated with new writes and added to the new
// data section.
func extendDataSection(
	w io.Writer,
	startOffset int64,
	startCRC uint32,
	attachments []*mcap.Attachment,
	metadata []*mcap.Metadata,
) (int64, []*mcap.AttachmentIndex, []*mcap.MetadataIndex, error) {
	cw := newChecksummingWriteCounter(w, startCRC)
	writer, err := mcap.NewWriter(cw, &mcap.WriterOptions{
		SkipMagic: true,
	})
	if err != nil {
		return cw.Count(), nil, nil, fmt.Errorf("failed to construct writer: %w", err)
	}
	attachmentIndexes := make([]*mcap.AttachmentIndex, len(attachments))
	for _, attachment := range attachments {
		offset := cw.Count()
		attachmentIndex := &mcap.AttachmentIndex{
			Offset:     uint64(offset) + uint64(startOffset),
			LogTime:    attachment.LogTime,
			CreateTime: attachment.CreateTime,
			DataSize:   attachment.DataSize,
			Name:       attachment.Name,
			MediaType:  attachment.MediaType,
		}
		err := writer.WriteAttachment(attachment)
		if err != nil {
			return cw.Count(), nil, nil, fmt.Errorf("failed to write attachment: %w", err)
		}
		attachmentIndex.Length = uint64(cw.Count() - offset)
		attachmentIndexes = append(attachmentIndexes, attachmentIndex)
	}

	metadataIndexes := make([]*mcap.MetadataIndex, len(metadata))
	for _, metadata := range metadata {
		offset := cw.Count()
		metadataIndex := &mcap.MetadataIndex{
			Offset: uint64(offset) + uint64(startOffset),
			Name:   metadata.Name,
		}
		err := writer.WriteMetadata(metadata)
		if err != nil {
			return cw.Count(), nil, nil, fmt.Errorf("failed to write metadata: %w", err)
		}
		metadataIndex.Length = uint64(cw.Count() - offset)
		metadataIndexes = append(metadataIndexes, metadataIndex)
	}
	// Only compute a CRC if the initial CRC is nonzero, otherwise leave it as zero.
	var dataSectionCRC uint32
	if startCRC != 0 {
		dataSectionCRC = cw.CRC()
	}
	newDataEnd := &mcap.DataEnd{
		DataSectionCRC: dataSectionCRC,
	}
	err = writer.WriteDataEnd(newDataEnd)
	if err != nil {
		return cw.Count(), nil, nil, fmt.Errorf("failed to write data end: %w", err)
	}
	return cw.Count(), attachmentIndexes, metadataIndexes, nil
}
