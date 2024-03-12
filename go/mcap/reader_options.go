package mcap

import (
	"fmt"
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

	StartNanos uint64
	EndNanos   uint64
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

// AfterNanos limits messages yielded by the reader to those with log times after this timestamp.
func AfterNanos(start uint64) ReadOpt {
	return func(ro *ReadOptions) error {
		if ro.EndNanos < start {
			return fmt.Errorf("end cannot come before start")
		}
		ro.StartNanos = start
		return nil
	}
}

// BeforeNanos limits messages yielded by the reader to those with log times before this timestamp.
func BeforeNanos(end uint64) ReadOpt {
	return func(ro *ReadOptions) error {
		if end < ro.StartNanos {
			return fmt.Errorf("end cannot come before start")
		}
		ro.EndNanos = end
		return nil
	}
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
