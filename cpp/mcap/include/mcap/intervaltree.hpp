#pragma once

// Adapted from <https://github.com/ekg/intervaltree/blob/master/IntervalTree.h>
// Original implementation copyright (c) 2011 Erik Garrison with the following
// license:

// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
// of the Software, and to permit persons to whom the Software is furnished to do
// so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#include <algorithm>
#include <cassert>
#include <iostream>
#include <limits>
#include <memory>
#include <vector>

namespace mcap {

template <class Scalar, typename Value>
class Interval {
public:
  Scalar start;
  Scalar stop;
  Value value;
  Interval(const Scalar& s, const Scalar& e, const Value& v)
      : start(std::min(s, e))
      , stop(std::max(s, e))
      , value(v) {}
};

// template <class Scalar, typename Value>
// Value intervalStart(const Interval<Scalar, Value>& i) {
//   return i.start;
// }

// template <class Scalar, typename Value>
// Value intervalStop(const Interval<Scalar, Value>& i) {
//   return i.stop;
// }

template <class Scalar, typename Value>
std::ostream& operator<<(std::ostream& out, const Interval<Scalar, Value>& i) {
  out << "Interval(" << i.start << ", " << i.stop << "): " << i.value;
  return out;
}

template <class Scalar, class Value>
class IntervalTree {
public:
  using interval = Interval<Scalar, Value>;
  using interval_vector = std::vector<interval>;

  struct IntervalStartCmp {
    bool operator()(const interval& a, const interval& b) {
      return a.start < b.start;
    }
  };

  struct IntervalStopCmp {
    bool operator()(const interval& a, const interval& b) {
      return a.stop < b.stop;
    }
  };

  IntervalTree()
      : left_(nullptr)
      , right_(nullptr)
      , center_(0) {}

  ~IntervalTree() = default;

  std::unique_ptr<IntervalTree> clone() const {
    return std::unique_ptr<IntervalTree>(new IntervalTree(*this));
  }

  IntervalTree(const IntervalTree& other)
      : intervals_(other.intervals_)
      , left_(other.left ? other.left_->clone() : nullptr)
      , right_(other.right_ ? other.right_->clone() : nullptr)
      , center_(other.center_) {}

  IntervalTree& operator=(IntervalTree&&) = default;
  IntervalTree(IntervalTree&&) = default;

  IntervalTree& operator=(const IntervalTree& other) {
    center_ = other.center_;
    intervals_ = other.intervals_;
    left_ = other.left_ ? other.left_->clone() : nullptr;
    right_ = other.right_ ? other.right_->clone() : nullptr;
    return *this;
  }

  IntervalTree(interval_vector&& ivals, std::size_t depth = 16, std::size_t minbucket = 64,
               std::size_t maxbucket = 512, Scalar leftextent = 0, Scalar rightextent = 0)
      : left_(nullptr)
      , right_(nullptr) {
    --depth;
    const auto minmaxStop = std::minmax_element(ivals.begin(), ivals.end(), IntervalStopCmp());
    const auto minmaxStart = std::minmax_element(ivals.begin(), ivals.end(), IntervalStartCmp());
    if (!ivals.empty()) {
      center_ = (minmaxStart.first->start + minmaxStop.second->stop) / 2;
    }
    if (leftextent == 0 && rightextent == 0) {
      // sort intervals by start
      std::sort(ivals.begin(), ivals.end(), IntervalStartCmp());
    } else {
      assert(std::is_sorted(ivals.begin(), ivals.end(), IntervalStartCmp()));
    }
    if (depth == 0 || (ivals.size() < minbucket && ivals.size() < maxbucket)) {
      std::sort(ivals.begin(), ivals.end(), IntervalStartCmp());
      intervals_ = std::move(ivals);
      return;
    } else {
      Scalar leftp = 0;
      Scalar rightp = 0;

      if (leftextent || rightextent) {
        leftp = leftextent;
        rightp = rightextent;
      } else {
        leftp = ivals.front().start;
        rightp = std::max_element(ivals.begin(), ivals.end(), IntervalStopCmp())->stop;
      }

      interval_vector lefts;
      interval_vector rights;

      for (const auto i = ivals.begin(); i != ivals.end(); ++i) {
        const interval& interval = *i;
        if (interval.stop < center_) {
          lefts.push_back(interval);
        } else if (interval.start > center_) {
          rights.push_back(interval);
        } else {
          assert(interval.start <= center_);
          assert(center_ <= interval.stop);
          intervals_.push_back(interval);
        }
      }

      if (!lefts.empty()) {
        left_.reset(
          new IntervalTree(std::move(lefts), depth, minbucket, maxbucket, leftp, center_));
      }
      if (!rights.empty()) {
        right_.reset(
          new IntervalTree(std::move(rights), depth, minbucket, maxbucket, center_, rightp));
      }
    }
  }

  // Call f on all intervals near the range [start, stop]:
  template <class UnaryFunction>
  void visit_near(const Scalar& start, const Scalar& stop, UnaryFunction f) const {
    if (!intervals_.empty() && !(stop < intervals_.front().start)) {
      for (auto& i : intervals_) {
        f(i);
      }
    }
    if (left_ && start <= center_) {
      left_->visit_near(start, stop, f);
    }
    if (right_ && stop >= center_) {
      right_->visit_near(start, stop, f);
    }
  }

  // Call f on all intervals overlapping pos
  template <class UnaryFunction>
  void visit_overlapping(const Scalar& pos, UnaryFunction f) const {
    visit_overlapping(pos, pos, f);
  }

  // Call f on all intervals overlapping [start, stop]
  template <class UnaryFunction>
  void visit_overlapping(const Scalar& start, const Scalar& stop, UnaryFunction f) const {
    auto filterF = [&](const interval& interval) {
      if (interval.stop >= start && interval.start <= stop) {
        // Only apply f if overlapping
        f(interval);
      }
    };
    visit_near(start, stop, filterF);
  }

  // Call f on all intervals contained within [start, stop]
  template <class UnaryFunction>
  void visit_contained(const Scalar& start, const Scalar& stop, UnaryFunction f) const {
    auto filterF = [&](const interval& interval) {
      if (start <= interval.start && interval.stop <= stop) {
        f(interval);
      }
    };
    visit_near(start, stop, filterF);
  }

  bool empty() const {
    if (left_ && !left_->empty()) {
      return false;
    }
    if (!intervals_.empty()) {
      return false;
    }
    if (right_ && !right_->empty()) {
      return false;
    }
    return true;
  }

  template <class UnaryFunction>
  void visit_all(UnaryFunction f) const {
    if (left_) {
      left_->visit_all(f);
    }
    std::for_each(intervals_.begin(), intervals_.end(), f);
    if (right_) {
      right_->visit_all(f);
    }
  }

  std::pair<Scalar, Scalar> extent() const {
    struct Extent {
      std::pair<Scalar, Scalar> x = {std::numeric_limits<Scalar>::max(),
                                     std::numeric_limits<Scalar>::min()};
      void operator()(const interval& interval) {
        x.first = std::min(x.first, interval.start);
        x.second = std::max(x.second, interval.stop);
      }
    };
    Extent extent;

    visit_all([&](const interval& interval) {
      extent(interval);
    });
    return extent.x;
  }

private:
  interval_vector intervals_;
  std::unique_ptr<IntervalTree> left_;
  std::unique_ptr<IntervalTree> right_;
  Scalar center_;
};

}  // namespace mcap
