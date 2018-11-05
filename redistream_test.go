package redistream

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func getClient() *redis.Client {
	addr := "localhost:16379"
	fmt.Printf("connecting to %s\n", addr)
	r := redis.NewClient(&redis.Options{
		Addr: addr})
	if _, err := r.Ping().Result(); err != nil {
		panic(fmt.Sprintf("connection to %s failed\n", addr))
	}
	return r
}

func TestProduceConsume(t *testing.T) {
	r := getClient()
	c := WrapClient(r, Config{
		MaxLenApprox: 1000,
		Block:        5 * time.Second,
		Count:        1})
	stream := "TestRedistream"
	r.Del(stream)
	entry := Entry{
		Hash: map[string]interface{}{
			"foo": "bar",
			"baz": "zip"}}
	c.Produce(ProduceArgs{
		Stream: stream,
		Entry:  entry})
	consumer := Consumer{
		Group: "redistream",
		Name:  "default"}
	smap, err := c.Consume(ConsumeArgs{
		Consumer: consumer,
		Streams:  []string{stream, ">"}})
	if err != nil {
		panic(err)
	}
	observed := Entry{}
	if len(smap[stream]) > 0 {
		observed = smap[stream][0]
	}
	assert.Equal(t, entry.Hash, observed.Hash)
}
