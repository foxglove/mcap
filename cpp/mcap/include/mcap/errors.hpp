#pragma once

#include <string>

namespace mcap {

/**
 * @brief Status codes for MCAP readers and writers.
 */
enum class StatusCode {
  Success = 0,
  NotOpen,
  InvalidSchemaId,
  InvalidChannelId,
  FileTooSmall,
  ReadFailed,
  MagicMismatch,
  InvalidFile,
  InvalidRecord,
  InvalidOpCode,
  InvalidChunkOffset,
  InvalidFooter,
  DecompressionFailed,
  DecompressionSizeMismatch,
  UnrecognizedCompression,
  OpenFailed,
  MissingStatistics,
};

/**
 * @brief Wraps a status code and string message carrying additional context.
 */
struct [[nodiscard]] Status {
  StatusCode code;
  std::string message;

  Status()
      : code(StatusCode::Success) {}

  Status(StatusCode code)
      : code(code) {
    switch (code) {
      case StatusCode::Success:
        break;
      case StatusCode::NotOpen:
        message = "not open";
        break;
      case StatusCode::InvalidSchemaId:
        message = "invalid schema id";
        break;
      case StatusCode::InvalidChannelId:
        message = "invalid channel id";
        break;
      case StatusCode::FileTooSmall:
        message = "file too small";
        break;
      case StatusCode::ReadFailed:
        message = "read failed";
        break;
      case StatusCode::MagicMismatch:
        message = "magic mismatch";
        break;
      case StatusCode::InvalidFile:
        message = "invalid file";
        break;
      case StatusCode::InvalidRecord:
        message = "invalid record";
        break;
      case StatusCode::InvalidOpCode:
        message = "invalid opcode";
        break;
      case StatusCode::InvalidChunkOffset:
        message = "invalid chunk offset";
        break;
      case StatusCode::InvalidFooter:
        message = "invalid footer";
        break;
      case StatusCode::DecompressionFailed:
        message = "decompression failed";
        break;
      case StatusCode::DecompressionSizeMismatch:
        message = "decompression size mismatch";
        break;
      case StatusCode::UnrecognizedCompression:
        message = "unrecognized compression";
        break;
      case StatusCode::OpenFailed:
        message = "open failed";
        break;
      case StatusCode::MissingStatistics:
        message = "missing statistics";
        break;
      default:
        message = "unknown";
        break;
    }
  }

  Status(StatusCode code, const std::string& message)
      : code(code)
      , message(message) {}

  bool ok() const {
    return code == StatusCode::Success;
  }
};

}  // namespace mcap
