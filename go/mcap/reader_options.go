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

	// StartNanos is the resolved inclusive lower bound on message log times: messages with
	// LogTime >= StartNanos are yielded. Prefer setting it through the StartsAtNanos or
	// StartsAfterNanos options.
	StartNanos uint64
	// EndNanos is the resolved exclusive upper bound on message log times: messages with
	// LogTime < EndNanos are yielded. Prefer setting it through the EndsAtNanos or
	// EndsBeforeNanos options.
	EndNanos uint64
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

// StartsAtNanos limits messages yielded by the reader to those with log times at or after this
// timestamp (inclusive lower bound). A later start option overrides an earlier one.
func StartsAtNanos(start uint64) ReadOpt {
	return func(ro *ReadOptions) error {
		if ro.EndNanos < start {
			return fmt.Errorf("end cannot come before start")
		}
		ro.StartNanos = start
		return nil
	}
}

// StartsAfterNanos limits messages yielded by the reader to those with log times strictly after
// this timestamp (exclusive lower bound). Log times are integer nanoseconds, so this is
// StartsAtNanos(start + 1). Passing math.MaxUint64 yields no messages, as no log time is
// strictly after it: the resolved StartNanos saturates to math.MaxUint64, and a message logged
// at exactly that time is always excluded by the exclusive upper bound (see EndsAtNanos). A
// later start option overrides an earlier one.
func StartsAfterNanos(start uint64) ReadOpt {
	return func(ro *ReadOptions) error {
		if start != math.MaxUint64 {
			start++
		}
		if ro.EndNanos < start {
			return fmt.Errorf("end cannot come before start")
		}
		ro.StartNanos = start
		return nil
	}
}

// EndsAtNanos limits messages yielded by the reader to those with log times at or before this
// timestamp (inclusive upper bound). Log times are integer nanoseconds, so the range
// [start, end] is [start, end+1). Passing math.MaxUint64 saturates: every message is yielded
// except one logged at exactly math.MaxUint64, which the uint64 bound representation cannot
// include (the same pre-existing limit applies to the unfiltered default). A later end option
// overrides an earlier one.
func EndsAtNanos(end uint64) ReadOpt {
	return func(ro *ReadOptions) error {
		if end < ro.StartNanos {
			return fmt.Errorf("end cannot come before start")
		}
		if end != math.MaxUint64 {
			end++
		}
		ro.EndNanos = end
		return nil
	}
}

// EndsBeforeNanos limits messages yielded by the reader to those with log times strictly before
// this timestamp (exclusive upper bound). A later end option overrides an earlier one.
func EndsBeforeNanos(end uint64) ReadOpt {
	return func(ro *ReadOptions) error {
		if end < ro.StartNanos {
			return fmt.Errorf("end cannot come before start")
		}
		ro.EndNanos = end
		return nil
	}
}

// AfterNanos limits messages yielded by the reader to those with log times at or after this
// timestamp. Despite the name, the bound is inclusive: messages logged exactly at this
// timestamp are yielded.
//
// Deprecated: use StartsAtNanos, which has the same behavior and says so.
func AfterNanos(start uint64) ReadOpt {
	return StartsAtNanos(start)
}

// BeforeNanos limits messages yielded by the reader to those with log times strictly before this
// timestamp (exclusive upper bound).
//
// Deprecated: use EndsBeforeNanos, which has the same behavior.
func BeforeNanos(end uint64) ReadOpt {
	return EndsBeforeNanos(end)
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
