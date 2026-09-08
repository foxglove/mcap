#pragma once
//
// concurrent_file_reader.hpp
//
// A thread-safe, concurrent-read IReadable backed by POSITIONED file reads:
// pread() on POSIX, ReadFile() with an OVERLAPPED offset on Windows. Because the
// read offset is passed per call (no shared file cursor), many threads can read
// different ranges of the same file at once -- which is what the ParallelReader's
// workers need. Reads go through the normal page cache + kernel readahead, so they
// stay cache-resident on large files (no fault-driven I/O stalls) and surface I/O
// errors as a Status rather than crashing, while staying concurrency-safe. It is
// also a safe drop-in for the serial reader, since it honors the same IReadable
// contract as FileReader.
//
// IReadable contract: read() returns a pointer into a THREAD-LOCAL buffer that
// stays valid until the next read() ON THE SAME THREAD (matching FileReader's
// single-buffer "valid until next read()" semantics, per-thread). Do not hold a
// returned pointer across another read() on the same thread.
//
#include "reader.hpp"  // IReadable, Status, StatusCode
#include <algorithm>
#include <string>
#include <vector>

#if defined(_WIN32)
// cspell:ignore NOMINMAX
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  ifndef NOMINMAX
#    define NOMINMAX  // windows.h defines min/max macros otherwise, breaking std::min/std::max
#  endif
#  include <windows.h>
#else
#  include <sys/stat.h>

#  include <cerrno>
#  include <fcntl.h>
#  include <unistd.h>
#endif

namespace mcap {

class ConcurrentFileReader final : public IReadable {
public:
  ConcurrentFileReader() = default;
  ~ConcurrentFileReader() override {
    close();
  }
  ConcurrentFileReader(const ConcurrentFileReader&) = delete;
  ConcurrentFileReader& operator=(const ConcurrentFileReader&) = delete;

  Status open(std::string_view path) {
    close();
    const std::string p(path);
#if defined(_WIN32)
    // FILE_FLAG_OVERLAPPED so each read can specify its own offset and the file
    // cursor is never shared -> safe for concurrent reads.
    handle_ = ::CreateFileA(p.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING,
                            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED, nullptr);
    if (handle_ == INVALID_HANDLE_VALUE) {
      return Status{StatusCode::OpenFailed, "failed to open " + p};
    }
    LARGE_INTEGER li{};
    if (!::GetFileSizeEx(handle_, &li)) {
      close();
      return Status{StatusCode::OpenFailed, "GetFileSizeEx failed for " + p};
    }
    size_ = static_cast<uint64_t>(li.QuadPart);
#else
    fd_ = ::open(p.c_str(), O_RDONLY);
    if (fd_ < 0) {
      return Status{StatusCode::OpenFailed, "failed to open " + p};
    }
    struct stat st {};
    if (::fstat(fd_, &st) != 0) {
      close();
      return Status{StatusCode::OpenFailed, "fstat failed for " + p};
    }
    size_ = static_cast<uint64_t>(st.st_size);
#endif
    if (size_ == 0) {
      close();
      return Status{StatusCode::FileTooSmall, "empty file " + p};
    }
    return StatusCode::Success;
  }

  void close() {
#if defined(_WIN32)
    if (handle_ != INVALID_HANDLE_VALUE) {
      ::CloseHandle(handle_);
      handle_ = INVALID_HANDLE_VALUE;
    }
#else
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
#endif
    size_ = 0;
  }

  uint64_t size() const override {
    return size_;
  }

  // pread / positioned ReadFile take the offset per call, so this is safe to call
  // concurrently from many threads.
  bool supportsConcurrentRead() const override {
    return true;
  }

  uint64_t read(std::byte** output, uint64_t offset, uint64_t size) override {
    if (offset >= size_) {
      return 0;
    }
    const uint64_t want = std::min(size, size_ - offset);
    std::vector<std::byte>& buf = threadBuffer();
    if (buf.size() < want) {
      buf.resize(want);
    }
    *output = buf.data();
    return readAt(buf.data(), offset, want);
  }

private:
  static std::vector<std::byte>& threadBuffer() {
    thread_local std::vector<std::byte> b;
    return b;
  }

#if defined(_WIN32)
  // One reusable manual-reset event per thread for the OVERLAPPED wait, so
  // concurrent reads on the same handle never share a completion object.
  static HANDLE threadEvent() {
    struct Holder {
      HANDLE ev = ::CreateEventA(nullptr, TRUE, FALSE, nullptr);
      ~Holder() {
        if (ev) ::CloseHandle(ev);
      }
    };
    thread_local Holder holder;
    return holder.ev;
  }

  uint64_t readAt(std::byte* dst, uint64_t offset, uint64_t want) {
    if (handle_ == INVALID_HANDLE_VALUE) {
      return 0;
    }
    const HANDLE ev = threadEvent();
    uint64_t got = 0;
    while (got < want) {
      const uint64_t pos = offset + got;
      const DWORD chunk = static_cast<DWORD>(std::min<uint64_t>(want - got, 1u << 30));
      OVERLAPPED ov{};
      ov.Offset = static_cast<DWORD>(pos & 0xFFFFFFFFull);
      ov.OffsetHigh = static_cast<DWORD>(pos >> 32);
      ov.hEvent = ev;
      DWORD n = 0;
      if (::ReadFile(handle_, dst + got, chunk, &n, &ov)) {
        if (n == 0) break;  // EOF
        got += n;
        continue;
      }
      if (::GetLastError() == ERROR_IO_PENDING) {
        if (!::GetOverlappedResult(handle_, &ov, &n, TRUE)) {
          break;  // includes ERROR_HANDLE_EOF
        }
        if (n == 0) break;
        got += n;
        continue;
      }
      break;  // ERROR_HANDLE_EOF or a real error
    }
    return got;
  }
#else
  uint64_t readAt(std::byte* dst, uint64_t offset, uint64_t want) {
    if (fd_ < 0) {
      return 0;
    }
    uint64_t got = 0;
    while (got < want) {
      const ssize_t n =
        ::pread(fd_, dst + got, static_cast<size_t>(want - got), static_cast<off_t>(offset + got));
      if (n < 0) {
        if (errno == EINTR) continue;  // retry interrupted syscall
        break;
      }
      if (n == 0) break;  // EOF
      got += static_cast<uint64_t>(n);
    }
    return got;
  }
#endif

#if defined(_WIN32)
  HANDLE handle_ = INVALID_HANDLE_VALUE;
#else
  int fd_ = -1;
#endif
  uint64_t size_ = 0;
};

}  // namespace mcap
