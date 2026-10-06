#pragma once
//
// thread_pool.hpp
//
// Minimal FIFO thread pool (C++17, no external deps). Workers pull jobs in
// submission order. Used by the parallel reader to decompress chunks. The pool
// joins all workers on destruction.
//
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace mcap::internal {

class ThreadPool {
public:
  explicit ThreadPool(unsigned n) {
    if (n == 0) {
      n = std::thread::hardware_concurrency();
      if (n == 0) n = 4;
    }
    workers_.reserve(n);
    for (unsigned i = 0; i < n; i++) {
      workers_.emplace_back([this] {
        run();
      });
    }
  }

  ~ThreadPool() {
    shutdown();
  }

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  void submit(std::function<void()> job) {
    {
      std::lock_guard<std::mutex> lk(m_);
      jobs_.push(std::move(job));
    }
    cv_.notify_one();
  }

  void shutdown() {
    {
      std::lock_guard<std::mutex> lk(m_);
      if (stopping_) return;
      stopping_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_) {
      if (t.joinable()) t.join();
    }
    workers_.clear();
  }

  unsigned size() const {
    return unsigned(workers_.size());
  }

private:
  void run() {
    for (;;) {
      std::function<void()> job;
      {
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait(lk, [this] {
          return stopping_ || !jobs_.empty();
        });
        if (stopping_ && jobs_.empty()) return;
        job = std::move(jobs_.front());
        jobs_.pop();
      }
      job();
    }
  }

  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> jobs_;
  std::mutex m_;
  std::condition_variable cv_;
  bool stopping_ = false;
};

}  // namespace mcap::internal
