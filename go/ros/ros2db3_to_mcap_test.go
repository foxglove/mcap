package ros

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/foxglove/mcap/go/libmcap"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestDB3MCAPConversion(t *testing.T) {
	db3file := "../../testdata/db3/chatter.db3"
	buf := &bytes.Buffer{}
	db, err := sql.Open("sqlite3", db3file)
	assert.Nil(t, err)

	opts := &libmcap.WriterOptions{
		IncludeCRC:  true,
		Chunked:     true,
		ChunkSize:   1024,
		Compression: "lz4",
	}

	err = DB3ToMCAP(buf, db, opts, []string{"./testdata/galactic"})
	assert.Nil(t, err)

	reader, err := libmcap.NewReader(bytes.NewReader(buf.Bytes()))
	assert.Nil(t, err)

	info, err := reader.Info()
	assert.Nil(t, err)
	assert.Equal(t, uint64(7), info.Statistics.MessageCount)
	assert.Equal(t, 1, len(info.Channels))
	assert.Equal(t, "/chatter", info.Channels[1].Topic)
}
