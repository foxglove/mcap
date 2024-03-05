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
	Start    uint64
	End      uint64
	Topics   []string
	UseIndex bool
	Order    ReadOrder

	MetadataCallback func(*Metadata) error
}

type ReadOpt func(*ReadOptions) error

func After(start uint64) ReadOpt {
	return func(ro *ReadOptions) error {
		if ro.End < start {
			return fmt.Errorf("end cannot come before start")
		}
		ro.Start = start
		return nil
	}
}

func Before(end uint64) ReadOpt {
	return func(ro *ReadOptions) error {
		if end < ro.Start {
			return fmt.Errorf("end cannot come before start")
		}
		ro.End = end
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
