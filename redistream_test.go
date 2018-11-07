package redistream

import (
	"context"
	"fmt"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMerge(t *testing.T) {
	hash := map[string]interface{}{
		"foo": "bar",
		"baz": "zip"}
	smap := StreamMap{
		"stream_one": []Entry{
			{ID: "0-0", Hash: hash},
			{ID: "1-1", Hash: hash},
			{ID: "2-0", Hash: hash},
			{ID: "3-0", Hash: hash, Meta: &EntryMeta{Stream: "stream_one"}}},
		"stream_two": []Entry{
			{ID: "0-1", Hash: hash},
			{ID: "1-0", Hash: hash},
			{ID: "2-1", Hash: hash},
			{ID: "3-0", Hash: hash, Meta: &EntryMeta{Stream: "stream_two"}}},
	}
	common := []Entry{
		{ID: "0-0", Hash: hash},
		{ID: "0-1", Hash: hash},
		{ID: "1-0", Hash: hash},
		{ID: "1-1", Hash: hash},
		{ID: "2-0", Hash: hash},
		{ID: "2-1", Hash: hash}}

	// "stream_two" first
	expected := append(common, []Entry{
		{ID: "3-0", Hash: hash, Meta: &EntryMeta{Stream: "stream_two"}},
		{ID: "3-0", Hash: hash, Meta: &EntryMeta{Stream: "stream_one"}}}...)
	actual, err := smap.Merge(&[]string{"stream_two", "stream_one"})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expected, actual)

	// "stream_one" first
	expected = append(common, []Entry{
		{ID: "3-0", Hash: hash, Meta: &EntryMeta{Stream: "stream_one"}},
		{ID: "3-0", Hash: hash, Meta: &EntryMeta{Stream: "stream_two"}}}...)
	actual, err = smap.Merge(&[]string{"stream_one", "stream_two"})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expected, actual)

	// no merge priority
	actual, err = smap.Merge(nil)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, common, actual[0:6])
}

func TestProduceConsume(t *testing.T) {
	r, cleanup := getClient()
	defer cleanup()
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
		t.Fatal(err)
	}
	observed := Entry{}
	if len(smap[stream]) > 0 {
		observed = smap[stream][0]
	}
	assert.Equal(t, entry.Hash, observed.Hash)
}

func TestProcess(t *testing.T) {
	r, cleanup := getClient()
	defer cleanup()
	conf := Config{
		MaxLenApprox: 1000,
		Block:        5 * time.Second,
		Count:        50}
	c := WrapClient(r, conf)
	stream := "TestRedistream"
	r.Del(stream)
	entry := Entry{
		Hash: map[string]interface{}{
			"foo": "bar",
			"zip": "zap",
			"i":   0}}
	for i := 0; i < 10; i++ {
		entry.Hash["i"] = i
		c.Produce(ProduceArgs{
			Stream: stream,
			Entry:  entry})
	}
	consumer := Consumer{
		Group: "redistream",
		Name:  "default"}
	smap, err := c.Consume(ConsumeArgs{
		Consumer: consumer,
		Streams:  []string{stream, ">"}})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := smap.Merge(nil)
	if err != nil {
		panic(err)
	}
	str := ""
	for i, e := range entries {
		if i < len(entries)-1 {
			str = fmt.Sprintf("%s %s(%s),", str, e.ID, e.Hash["i"])
		} else {
			str = fmt.Sprintf("%s %s(%s)", str, e.ID, e.Hash["i"])
		}
	}
	streamDest := stream + "_2"
	toentry := []Entry{
		Entry{
			Hash: map[string]interface{}{
				"data": str,
			},
			Meta: &EntryMeta{Stream: streamDest},
		},
	}
	c.Process(ProcessArgs{From: entries, To: toentry})

	// Dest stream is the right length and entry hash matches the input?
	destEntries, err := c.Consume(ConsumeArgs{
		Consumer: consumer,
		Streams:  []string{streamDest, ">"}})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(destEntries))
	merged, err := destEntries.Merge(nil)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, toentry[0].Hash, merged[0].Hash)

	// XACK worked?
	srcPending, err := r.XPending(stream, consumer.Group).Result()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int64(0), srcPending.Count)
}

func removeMatchingContainers(cli *client.Client, ctx context.Context, exactName string) {
	containers, err := cli.ContainerList(ctx, types.ContainerListOptions{
		Filters: filters.NewArgs(filters.Arg("name", fmt.Sprintf("^/%s$", exactName))),
	})
	if err != nil {
		panic(err)
	}
	if len(containers) > 0 {
		for _, c := range containers {
			err = cli.ContainerRemove(ctx, c.ID, types.ContainerRemoveOptions{
				Force: true,
			})
			if err != nil {
				panic(err)
			}
			fmt.Printf("old redis:5.0.0 removed (ID: %.12s, Name: %s)\n", c.ID, exactName)
		}
	}
}

func runRedisDocker() func() {
	// get context
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.WithVersion("1.38"))
	if err != nil {
		panic(err)
	}
	err = client.FromEnv(cli)
	if err != nil {
		panic(err)
	}

	// pull image
	imageName := "redis:5.0.0"
	_, err = cli.ImagePull(ctx, imageName, types.ImagePullOptions{})
	if err != nil {
		panic(err)
	}

	// create container
	containerName := "redistream_test__redis"
	removeMatchingContainers(cli, ctx, containerName)
	// io.Copy(os.Stdout, out)
	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image: imageName,
	}, &container.HostConfig{
		PortBindings: nat.PortMap{
			"6379/tcp": []nat.PortBinding{{
				HostIP:   "127.0.0.1",
				HostPort: "16379",
			}}}}, nil, containerName)
	if err != nil {
		panic(err)
	}

	// start container
	if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		panic(err)
	}
	fmt.Printf("redis:5.0.0 started (ID: %.12s, Name: %s)\n", resp.ID, containerName)
	return func() {
		err := cli.ContainerRemove(ctx, resp.ID, types.ContainerRemoveOptions{
			Force: true,
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf("redis:5.0.0 removed (ID: %.12s, Name: %s)\n", resp.ID, containerName)
	}
}

func getClient() (*redis.Client, func()) {
	cleanup := runRedisDocker()
	// give redis a bit to come online
	time.Sleep(time.Millisecond * 50)
	defer func() {
		if r := recover(); r != nil {
			cleanup()
			panic(r)
		}
	}()
	addr := "localhost:16379"
	r := redis.NewClient(&redis.Options{
		PoolSize: 10,
		Addr:     addr})
	if _, err := r.Ping().Result(); err != nil {
		panic(err)
	}
	return r, cleanup
}
