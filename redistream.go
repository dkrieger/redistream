package redistream

import (
	"github.com/go-redis/redis"
)

type StreamClient struct {
	redisClient *redis.Client
}

type Entry map[string]string

func (c *StreamClient) Consume(group, consumer string, count int, streams []string) [][]*Entry {
	// . . .
	return nil
}

func (c *StreamClient) Produce(entry *Entry) int {
	// . . .
	return 0
}

func WrapClient(redisClient *redis.Client) *StreamClient {
	return &StreamClient{
		redisClient,
	}
}
