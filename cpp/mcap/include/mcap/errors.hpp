#pragma once

#include <string>

namespace mcap {

enum class StatusCode {
  Success = 0,
  NotOpen = 1,
  InvalidChannelId = 2,
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
      case StatusCode::InvalidChannelId:
        message = "invalid channel id";
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
