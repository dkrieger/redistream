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

func (c *Client) Consume(consumer Consumer, streams []string, override *Config) (StreamMap, error) {
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
	streamEntries := StreamMap{}
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
	from := args.From
	to := args.To
	override := args.Override
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
	pipe := c.redisClient.TxPipeline()
	xAckCmds := []*redis.IntCmd{}
	for _, e := range from {
		cmd := pipe.XAck(e.Meta.Stream, e.Meta.Consumer.Group, e.ID)
		xAckCmds = append(xAckCmds, cmd)
	}
	xAddCmds := []*redis.StringCmd{}
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
