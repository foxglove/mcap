#pragma once

#include <string>
#include <system_error>

namespace mcap {

enum class ErrorCode {
  Success = 0,
  NotOpen = 1,
  InvalidChannelId = 2,
};

}  // namespace mcap

namespace std {
// Register mcap::ErrorCode with the standard error code system
template <>
struct is_error_code_enum<mcap::ErrorCode> : true_type {};
}  // namespace std

namespace mcap {

namespace detail {

// Define a custom error code category derived from std::error_category
class McapErrorCategory : public std::error_category {
public:
  virtual const char* name() const noexcept override final {
    return "McapError";
  }

  virtual std::string message(int c) const override final {
    switch (static_cast<ErrorCode>(c)) {
      case ErrorCode::Success:
        return "success";
      case ErrorCode::NotOpen:
        return "not open";
      case ErrorCode::InvalidChannelId:
        return "invalid channel id";
      default:
        return "unknown";
    }
  }

  virtual std::error_condition default_error_condition(int c) const noexcept override final {
    switch (static_cast<ErrorCode>(c)) {
      case ErrorCode::NotOpen:
        return make_error_condition(std::errc::bad_file_descriptor);
      case ErrorCode::InvalidChannelId:
        return make_error_condition(std::errc::invalid_argument);
      default:
        return std::error_condition(c, *this);
    }
  }
};

}  // namespace detail

const detail::McapErrorCategory& McapErrorCategory() {
  static detail::McapErrorCategory c;
  return c;
}

inline std::error_code make_error_code(ErrorCode e) {
  return {int(e), McapErrorCategory()};
}

}  // namespace mcap
