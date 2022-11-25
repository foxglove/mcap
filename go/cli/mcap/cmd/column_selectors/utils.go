package column_selectors

import (
	"errors"
	"io"

	"github.com/foxglove/mcap/go/mcap"
)

type channelIndexPair struct {
	channel *mcap.Channel
	index   int
}

func getTopicAverageSizes(reader *mcap.Reader) (map[string]float64, error) {
	iterator, err := reader.Messages()
	if err != nil {
		return nil, err
	}
	messageSizes := make(map[string][]int)
	for {
		_, channel, message, err := iterator.Next(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		messageSizes[channel.Topic] = append(messageSizes[channel.Topic], len(message.Data))
	}
	averageSizes := make(map[string]float64)
	for topic, sizes := range messageSizes {
		sum := 0
		for _, size := range sizes {
			sum += size
		}
		averageSizes[topic] = float64(sum) / float64(len(sizes))
	}
	return averageSizes, nil
}
