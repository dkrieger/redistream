/*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation; either version 2 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program; if not, see <http://www.gnu.org/licenses/>.
*
* Copyright (C) Doug Krieger <doug@debutdigital.com>, 2018
 */

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

func (c *Client) getOverridenConf(override *Config) *Config {
	conf := &Config{
		MaxLen:       c.conf.MaxLen,
		MaxLenApprox: c.conf.MaxLenApprox,
		Block:        c.conf.Block,
		Count:        c.conf.Count,
	}
	if override != nil {
		conf = &Config{
			MaxLen:       override.MaxLen,
			MaxLenApprox: override.MaxLenApprox,
			Block:        override.Block,
			Count:        override.Count,
		}
	}
	return conf
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

type StreamMap map[string][]Entry

func (s StreamMap) Flat() []Entry {
	entries := []Entry{}
	for _, e := range s {
		copy(entries, e)
	}
	return entries
}

type ConsumeArgs struct {
	Consumer Consumer
	Streams  []string
	Override *Config
}

func (c *Client) Consume(args ConsumeArgs) (StreamMap, error) {
	// parse args
	consumer := args.Consumer
	streams := args.Streams
	override := args.Override

	// XREADGROUP
	streamEntries := StreamMap{}
	conf := c.getOverridenConf(override)
	xreadgroupargs := &redis.XReadGroupArgs{
		Group:    consumer.Group,
		Consumer: consumer.Name,
		Streams:  streams,
		Count:    conf.Count,
		Block:    conf.Block,
	}
	c.ensureStreamsInit(xreadgroupargs)
	c.ensureGroupsInit(xreadgroupargs)
	res, err := c.redisClient.XReadGroup(xreadgroupargs).Result()
	// if err != nil && err.Error() != "redis: nil" {
	if err != nil {
		return streamEntries, err
	}

	// process results
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

func (c *Client) ensureStreamsInit(args *redis.XReadGroupArgs) {
	cli := c.redisClient
	script := `local t = redis.call("type", KEYS[1]) ` +
		`if t.ok == "none" then ` +
		`redis.call("xadd", KEYS[1], "0-1", "empty", "me") ` +
		`redis.call("xtrim", KEYS[1], "maxlen", 0) ` +
		`end return 1`
	var streams []string
	for i := 0; i < len(args.Streams); i++ {
		if i%2 == 0 {
			streams = append(streams, args.Streams[i])
		}
	}
	for _, stream := range streams {
		if _, err := cli.Eval(script, []string{stream}).Result(); err != nil {
			panic(err)
		}
	}
}

func (c *Client) ensureGroupsInit(args *redis.XReadGroupArgs) {
	cli := c.redisClient
	var streams [][]string
	for i := 0; i < len(args.Streams); i += 2 {
		streams = append(streams, args.Streams[i:i+2])
	}
	for _, stream := range streams {
		cli.XGroupCreate(stream[0], args.Group, "0").Result()
	}
}

type ProduceArgs struct {
	Stream   string
	Entry    Entry
	Override *Config
}

func (c *Client) Produce(args ProduceArgs) (string, error) {
	stream := args.Stream
	entry := args.Entry
	override := args.Override
	conf := c.getOverridenConf(override)
	ID, err := c.redisClient.XAdd(&redis.XAddArgs{
		Stream:       stream,
		ID:           entry.ID,
		Values:       entry.Hash,
		MaxLen:       conf.MaxLen,
		MaxLenApprox: conf.MaxLenApprox,
	}).Result()
	if err != nil {
		return ID, err
	}
	return ID, err
}

type ProcessArgs struct {
	From     []Entry
	To       []Entry
	Override *Config
}

type XAckResult struct {
	ID      string
	Success bool
}

// Process atomically `XACK`s and `XADD`s entries. For example, this can be
// used for migrating entries from one stream to another, processing a batch of
// entries into one dependent entry, or `XACK`ing entries without `XADD`ing
// any. Process can be thought of `XACK` with optional `XADD` side-effects.
// NOTE: this will panic on any errors whatsoever; transactions in redis cannot
// be rolled back, as antirez believes the client should be responsible for
// avoiding errors. Furthermore, `XACK` is an irreversible operation, so the
// client can't even manually roll it back. Thus, these errors should never
// happen in proper usage and are are not recoverable, matching up with the
// idiomatic meaning of `panic` in golang.
func (c *Client) Process(args ProcessArgs) ([]XAckResult, []string) {
	// parse args
	from := args.From
	to := args.To
	override := args.Override

	// build multi/exec pipeline
	pipe := c.redisClient.TxPipeline()
	// XACK
	xAckCmds := []*redis.IntCmd{}
	for _, e := range from {
		cmd := pipe.XAck(e.Meta.Stream, e.Meta.Consumer.Group, e.ID)
		xAckCmds = append(xAckCmds, cmd)
	}
	// XADD
	xAddCmds := []*redis.StringCmd{}
	conf := c.getOverridenConf(override)
	for _, e := range to {
		args := &redis.XAddArgs{
			Stream:       e.Meta.Stream,
			ID:           e.ID,
			Values:       e.Hash,
			MaxLen:       conf.MaxLen,
			MaxLenApprox: conf.MaxLenApprox,
		}
		cmd := pipe.XAdd(args)
		xAddCmds = append(xAddCmds, cmd)
	}

	// run multi/exec pipeline, process results
	pipe.Exec()
	acks := []XAckResult{}
	for i, cmd := range xAckCmds {
		count, err := cmd.Result()
		if err != nil {
			// XACK shouldn't error under normal usage. It might
			// mean the stream or group has been deleted.
			panic(err)
		}
		acks = append(acks, XAckResult{
			ID:      from[i].ID,
			Success: count == 1,
		})
	}
	ids := []string{}
	for _, cmd := range xAddCmds {
		id, err := cmd.Result()
		if err != nil {
			panic(err)
		}
		ids = append(ids, id)
	}
	return acks, ids
}
