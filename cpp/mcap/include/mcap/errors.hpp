#pragma once

#include <fmt/core.h>

#include <string>

namespace mcap {

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
  DecompressionFailed,
  DecompressionSizeMismatch,
  UnrecognizedCompression,
};

struct Status {
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
      case StatusCode::DecompressionFailed:
        message = "decompression failed";
        break;
      case StatusCode::DecompressionSizeMismatch:
        message = "decompression size mismatch";
        break;
      case StatusCode::UnrecognizedCompression:
        message = "unrecognized compression";
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

template <typename... T>
[[nodiscard]] inline std::string StrFormat(std::string_view msg, T&&... args) {
  return fmt::format(msg, std::forward<T>(args)...);
}

}  // namespace mcap
