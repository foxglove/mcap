package mcap

import (
	"fmt"
	"math"
)

type ReadOrder int

const (
	FileOrder           ReadOrder = 0
	LogTimeOrder        ReadOrder = 1
	ReverseLogTimeOrder ReadOrder = 2
)

type ReadOptions struct {
	// Deprecated: use StartNanos instead
	Start int64
	// Deprecated: use EndNanos instead
	End      int64
	Topics   []string
	UseIndex bool
	Order    ReadOrder

	MetadataCallback func(*Metadata) error

	// StartNanos is an inclusive lower bound on message log times. A bound set through
	// StartingAtNanos or StartingAfterNanos takes precedence over it.
	StartNanos uint64
	// EndNanos is an exclusive upper bound on message log times; zero means no upper bound. A
	// bound set through EndingAtNanos or EndingBeforeNanos takes precedence over it.
	EndNanos uint64

	// Bounds set through the StartingAt/StartingAfter/EndingAt/EndingBefore options.
	lower logTimeBound
	upper logTimeBound
}

// logTimeBoundKind says how a logTimeBound's value is to be compared, if at all.
type logTimeBoundKind uint8

const (
	logTimeBoundUnset logTimeBoundKind = iota
	logTimeBoundInclusive
	logTimeBoundExclusive
)

// logTimeBound is one side of a log time range, exactly as the caller provided it.
type logTimeBound struct {
	kind  logTimeBoundKind
	value uint64
}

// logTimeBounds is the log time range a read is limited to, with each bound kept as provided.
type logTimeBounds struct {
	lower logTimeBound
	upper logTimeBound
}

// logTimeBounds returns the option-set bounds, falling back to StartNanos/EndNanos on a side
// without one. Call Finalize first.
func (ro *ReadOptions) logTimeBounds() logTimeBounds {
	bounds := logTimeBounds{lower: ro.lower, upper: ro.upper}
	if bounds.lower.kind == logTimeBoundUnset {
		bounds.lower = logTimeBound{kind: logTimeBoundInclusive, value: ro.StartNanos}
	}
	if bounds.upper.kind == logTimeBoundUnset && ro.EndNanos != 0 {
		bounds.upper = logTimeBound{kind: logTimeBoundExclusive, value: ro.EndNanos}
	}
	return bounds
}

func (b logTimeBounds) lowerBoundIncludes(logTime uint64) bool {
	switch b.lower.kind {
	case logTimeBoundInclusive:
		return logTime >= b.lower.value
	case logTimeBoundExclusive:
		return logTime > b.lower.value
	default:
		return true
	}
}

func (b logTimeBounds) upperBoundIncludes(logTime uint64) bool {
	switch b.upper.kind {
	case logTimeBoundInclusive:
		return logTime <= b.upper.value
	case logTimeBoundExclusive:
		return logTime < b.upper.value
	default:
		return true
	}
}

// includesLogTime reports whether a message logged at logTime falls inside the range.
func (b logTimeBounds) includesLogTime(logTime uint64) bool {
	return b.lowerBoundIncludes(logTime) && b.upperBoundIncludes(logTime)
}

// overlapsLogTimes reports whether any log time in [first, last] falls inside the range.
func (b logTimeBounds) overlapsLogTimes(first, last uint64) bool {
	return b.lowerBoundIncludes(last) && b.upperBoundIncludes(first)
}

// isCrossed reports whether the upper bound lies strictly below the lower bound. An empty
// range, such as StartingAtNanos(5) with EndingBeforeNanos(5), is not crossed.
func (b logTimeBounds) isCrossed() bool {
	var firstIncluded uint64
	switch b.lower.kind {
	case logTimeBoundInclusive:
		firstIncluded = b.lower.value
	case logTimeBoundExclusive:
		if b.lower.value == math.MaxUint64 {
			return false // nothing is after math.MaxUint64: empty, not crossed
		}
		firstIncluded = b.lower.value + 1
	default:
		return false
	}
	var firstExcluded uint64
	switch b.upper.kind {
	case logTimeBoundExclusive:
		firstExcluded = b.upper.value
	case logTimeBoundInclusive:
		if b.upper.value == math.MaxUint64 {
			return false // no upper bound: cannot be crossed
		}
		firstExcluded = b.upper.value + 1
	default:
		return false
	}
	return firstIncluded > firstExcluded
}

func (ro *ReadOptions) Finalize() {
	if ro.StartNanos == 0 && ro.Start > 0 {
		ro.StartNanos = uint64(ro.Start)
	}
	if ro.EndNanos == 0 && ro.End > 0 {
		ro.EndNanos = uint64(ro.End)
	}
}

type ReadOpt func(*ReadOptions) error

// After limits messages yielded by the reader to those with log times after this timestamp.
//
// Deprecated: the int64 argument does not permit the full range of possible message timestamps,
// use AfterNanos instead.
func After(start int64) ReadOpt {
	return func(ro *ReadOptions) error {
		if ro.End < start {
			return fmt.Errorf("end cannot come before start")
		}
		ro.Start = start
		return nil
	}
}

// Before limits messages yielded by the reader to those with log times before this timestamp.
//
// Deprecated: the int64 argument does not permit the full range of possible message timestamps,
// use BeforeNanos instead.
func Before(end int64) ReadOpt {
	return func(ro *ReadOptions) error {
		if end < ro.Start {
			return fmt.Errorf("end cannot come before start")
		}
		ro.End = end
		return nil
	}
}

// StartingAtNanos limits messages yielded by the reader to those with log times at or after this
// timestamp (inclusive lower bound). A later start option overrides an earlier one.
func StartingAtNanos(start uint64) ReadOpt {
	return func(ro *ReadOptions) error {
		ro.lower = logTimeBound{kind: logTimeBoundInclusive, value: start}
		return nil
	}
}

// StartingAfterNanos limits messages yielded by the reader to those with log times strictly after
// this timestamp (exclusive lower bound). Passing math.MaxUint64 yields nothing and is not an
// error. A later start option overrides an earlier one.
func StartingAfterNanos(start uint64) ReadOpt {
	return func(ro *ReadOptions) error {
		ro.lower = logTimeBound{kind: logTimeBoundExclusive, value: start}
		return nil
	}
}

// EndingAtNanos limits messages yielded by the reader to those with log times at or before this
// timestamp (inclusive upper bound). Passing math.MaxUint64 means no upper bound. A later end
// option overrides an earlier one.
func EndingAtNanos(end uint64) ReadOpt {
	return func(ro *ReadOptions) error {
		ro.upper = logTimeBound{kind: logTimeBoundInclusive, value: end}
		return nil
	}
}

// EndingBeforeNanos limits messages yielded by the reader to those with log times strictly before
// this timestamp (exclusive upper bound). A later end option overrides an earlier one.
func EndingBeforeNanos(end uint64) ReadOpt {
	return func(ro *ReadOptions) error {
		ro.upper = logTimeBound{kind: logTimeBoundExclusive, value: end}
		return nil
	}
}

// AfterNanos limits messages yielded by the reader to those with log times at or after this
// timestamp. Despite the name, the bound is inclusive.
//
// Deprecated: use StartingAtNanos, which has the same behavior.
func AfterNanos(start uint64) ReadOpt {
	return StartingAtNanos(start)
}

// BeforeNanos limits messages yielded by the reader to those with log times strictly before this
// timestamp (exclusive upper bound).
//
// Deprecated: use EndingBeforeNanos, which has the same behavior.
func BeforeNanos(end uint64) ReadOpt {
	return EndingBeforeNanos(end)
}

func WithTopics(topics []string) ReadOpt {
	return func(ro *ReadOptions) error {
		ro.Topics = topics
		return nil
	}
}

func InOrder(order ReadOrder) ReadOpt {
	return func(ro *ReadOptions) error {
		if !ro.UseIndex && order != FileOrder {
			return fmt.Errorf("only file-order reads are supported when not using index")
		}
		ro.Order = order
		return nil
	}
}

func UsingIndex(useIndex bool) ReadOpt {
	return func(ro *ReadOptions) error {
		if ro.Order != FileOrder && !useIndex {
			return fmt.Errorf("only file-order reads are supported when not using index")
		}
		ro.UseIndex = useIndex
		return nil
	}
}

func WithMetadataCallback(callback func(*Metadata) error) ReadOpt {
	return func(ro *ReadOptions) error {
		ro.MetadataCallback = callback
		return nil
	}
}
