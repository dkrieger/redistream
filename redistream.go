// redistream provides higher-level client-side stream operations on top of
// redis stream semantics. Additionally, it provides wrappers on top of
// lower-level redis stream commands to do things like injecting defaults. By
// default, `github.com/go-redis/redis` is used to communicate with redis, but
// redigo support may be bolted on at a later time. To facilitate this,
// higher-level client-side stream operations are decoupled from the underlying
// redis client library by using structs defined in redistream for all
// higher-level operations. Wrappers on lower-level redis stream commands
// can/should be more tightly coupled with the underlying redis client library.
package redistream

import (
	"github.com/go-redis/redis"
	"time"
)

// Config based on redis.XAddArgs (go-redis)
type Config struct {
	MaxLen       int64
	MaxLenApprox int64
	Block        time.Duration
	Count        int64
}

type Client struct {
	redisClient *redis.Client
	conf        Config
}

func WrapClient(redisClient *redis.Client, conf Config) *Client {
	return &Client{
		redisClient,
		conf,
	}
}

// Entry is based off redis.XMessage (go-redis)
type Entry struct {
	ID   string
	Hash map[string]interface{}
	Meta *EntryMeta
}

type Consumer struct {
	Group string
	Name  string
}

type EntryMeta struct {
	Consumer Consumer
	Stream   string
}

func (c *Client) Consume(consumer Consumer, streams []string, override *Config) (map[string][]Entry, error) {
	conf := &Config{
		Count: c.conf.Count,
		Block: c.conf.Block,
	}
	if override != nil {
		conf = &Config{
			Count: override.Count,
			Block: override.Block,
		}
	}
	args := &redis.XReadGroupArgs{
		Group:    consumer.Group,
		Consumer: consumer.Name,
		Streams:  streams,
		Count:    conf.Count,
		Block:    conf.Block,
	}
	streamEntries := map[string][]Entry{}
	res, err := c.redisClient.XReadGroup(args).Result()
	if err != nil && err.Error() != "redis: nil" {
		return streamEntries, err
	}
	for _, stream := range res {
		entries := []Entry{}
		for _, msg := range stream.Messages {
			entry := Entry{
				ID:   msg.ID,
				Hash: msg.Values,
				Meta: &EntryMeta{
					Consumer: consumer,
					Stream:   stream.Stream,
				},
			}
			entries = append(entries, entry)
		}
		streamEntries[stream.Stream] = entries
	}
	return streamEntries, nil
}

func (c *Client) Produce(stream string, entry Entry, override *Config) (string, error) {
	conf := &Config{
		MaxLen:       c.conf.MaxLen,
		MaxLenApprox: c.conf.MaxLenApprox,
	}
	if override != nil {
		conf = &Config{
			MaxLen:       override.MaxLen,
			MaxLenApprox: override.MaxLenApprox,
		}
	}
	args := &redis.XAddArgs{
		Stream:       stream,
		ID:           entry.ID,
		Values:       entry.Hash,
		MaxLen:       conf.MaxLen,
		MaxLenApprox: conf.MaxLenApprox,
	}
	ID, err := c.redisClient.XAdd(args).Result()
	if err != nil {
		return ID, err
	}
	return ID, err
}

// Process atomically `XACK`s and `XADD`s entries. For example, this can be
// used for migrating entries from one stream to another, processing a batch of
// entries into one dependent entry, or `XACK`ing entries without `XADD`ing
// any. It can be thought of `XACK` with optional `XADD` side-effects.
func (c *Client) Process(from []Entry, to []Entry) {
	// . . .
}
