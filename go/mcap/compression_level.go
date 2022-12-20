package mcap

import (
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Compression level to use when compressing. Recommend using only the constant values to avoid
// breakage when making library updates.
type CompressionLevel int

const (
	CompressionFastest CompressionLevel = -20
	CompressionFast    CompressionLevel = -10
	CompressionDefault CompressionLevel = 0
	CompressionSlow    CompressionLevel = 10
	CompressionSlowest CompressionLevel = 20
)

func CompressionLevelFromString(level string) CompressionLevel {
	switch level {
	case "fastest":
		return CompressionFastest
	case "fast":
		return CompressionFast
	case "default":
		return CompressionDefault
	case "slow":
		return CompressionSlow
	case "slowest":
		return CompressionSlowest
	default:
		return CompressionDefault
	}
}

func (c CompressionLevel) lz4Level() lz4.CompressionLevel {
	switch c {
	case CompressionFastest:
		return lz4.Fast
	case CompressionFast:
		return lz4.Level3
	case CompressionDefault:
		return lz4.Level5
	case CompressionSlow:
		return lz4.Level7
	case CompressionSlowest:
		return lz4.Level9
	default:
		return CompressionDefault.lz4Level()
	}
}

func (c CompressionLevel) zstdLevel() zstd.EncoderLevel {
	switch c {
	case CompressionFastest:
		return zstd.SpeedFastest
	case CompressionFast:
		return zstd.SpeedFastest
	case CompressionDefault:
		return zstd.SpeedDefault
	case CompressionSlow:
		return zstd.SpeedBetterCompression
	case CompressionSlowest:
		return zstd.SpeedBestCompression
	default:
		return CompressionDefault.zstdLevel()
	}
}
