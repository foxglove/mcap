#pragma once
//
// mmap_reader.hpp
//
// An IReadable backed by a read-only memory mapping. read() returns a pointer
// directly into the mapping and never mutates shared state, so it is safe to
// call concurrently from many threads -- which is exactly what the parallel
// reader's decompression workers require. Pointers stay valid for the lifetime
// of the MmapReader (a stronger guarantee than the IReadable contract demands).
//
// POSIX (mmap) and Windows (CreateFileMapping/MapViewOfFile) implementations
// behind one interface.
//
#include "reader.hpp"  // IReadable, Status, StatusCode (included by mcap.hpp before us)
#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>

#if defined(_WIN32)
// cspell:ignore NOMINMAX
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  ifndef NOMINMAX
#    define NOMINMAX  // keep windows.h from defining min()/max() macros
#  endif
#  include <windows.h>
#else
#  include <sys/mman.h>
#  include <sys/stat.h>

#  include <fcntl.h>
#  include <unistd.h>
#endif

namespace mcap {

class MmapReader final : public IReadable {
public:
  MmapReader() = default;
  ~MmapReader() override {
    close();
  }

  MmapReader(const MmapReader&) = delete;
  MmapReader& operator=(const MmapReader&) = delete;

  // The mapping is immutable after open(), so read() is safe from many threads.
  bool supportsConcurrentRead() const override {
    return true;
  }

#if defined(_WIN32)
  Status open(std::string_view path) {
    close();
    const std::string p(path);
    fileHandle_ = ::CreateFileA(p.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING,
                                FILE_ATTRIBUTE_NORMAL, nullptr);
    if (fileHandle_ == INVALID_HANDLE_VALUE) {
      return Status{StatusCode::OpenFailed, "could not open " + p};
    }
    LARGE_INTEGER fileSize{};
    if (!::GetFileSizeEx(fileHandle_, &fileSize)) {
      close();
      return Status{StatusCode::OpenFailed, "GetFileSizeEx failed for " + p};
    }
    size_ = uint64_t(fileSize.QuadPart);
    if (size_ == 0) {
      close();
      return Status{StatusCode::FileTooSmall, "empty file " + p};
    }
    mapHandle_ = ::CreateFileMappingA(fileHandle_, nullptr, PAGE_READONLY, 0, 0, nullptr);
    if (mapHandle_ == nullptr) {
      close();
      return Status{StatusCode::OpenFailed, "CreateFileMapping failed for " + p};
    }
    void* view = ::MapViewOfFile(mapHandle_, FILE_MAP_READ, 0, 0, 0);
    if (view == nullptr) {
      close();
      return Status{StatusCode::OpenFailed, "MapViewOfFile failed for " + p};
    }
    base_ = static_cast<const std::byte*>(view);
    return StatusCode::Success;
  }

  void close() {
    if (base_ != nullptr) {
      ::UnmapViewOfFile(static_cast<const void*>(base_));
      base_ = nullptr;
    }
    if (mapHandle_ != nullptr) {
      ::CloseHandle(mapHandle_);
      mapHandle_ = nullptr;
    }
    if (fileHandle_ != INVALID_HANDLE_VALUE) {
      ::CloseHandle(fileHandle_);
      fileHandle_ = INVALID_HANDLE_VALUE;
    }
    size_ = 0;
  }
#else
  Status open(std::string_view path) {
    close();
    const std::string p(path);
    fd_ = ::open(p.c_str(), O_RDONLY);
    if (fd_ < 0) {
      return Status{StatusCode::OpenFailed, "could not open " + p};
    }
    struct stat st {};
    if (::fstat(fd_, &st) != 0) {
      close();
      return Status{StatusCode::OpenFailed, "fstat failed for " + p};
    }
    size_ = uint64_t(st.st_size);
    if (size_ == 0) {
      close();
      return Status{StatusCode::FileTooSmall, "empty file " + p};
    }
    void* m = ::mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0);
    if (m == MAP_FAILED) {
      close();
      return Status{StatusCode::OpenFailed, "mmap failed for " + p};
    }
    base_ = static_cast<const std::byte*>(m);
    // Forward sequential scans benefit from readahead.
    ::madvise(const_cast<void*>(static_cast<const void*>(base_)), size_, MADV_WILLNEED);
    return StatusCode::Success;
  }

  void close() {
    if (base_ != nullptr) {
      ::munmap(const_cast<void*>(static_cast<const void*>(base_)), size_);
      base_ = nullptr;
    }
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
    size_ = 0;
  }
#endif

  uint64_t size() const override {
    return size_;
  }

  // Thread-safe for concurrent calls: no shared mutable state.
  uint64_t read(std::byte** output, uint64_t offset, uint64_t size) override {
    if (base_ == nullptr || offset >= size_) {
      return 0;
    }
    const uint64_t available = size_ - offset;
    *output = const_cast<std::byte*>(base_ + offset);
    return std::min(size, available);
  }

private:
  const std::byte* base_ = nullptr;
  uint64_t size_ = 0;
#if defined(_WIN32)
  void* fileHandle_ = INVALID_HANDLE_VALUE;  // HANDLE
  void* mapHandle_ = nullptr;                // HANDLE
#else
  int fd_ = -1;
#endif
};

}  // namespace mcap
