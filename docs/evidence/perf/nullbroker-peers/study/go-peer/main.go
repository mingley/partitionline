// Public franz-go calls against the declared, independent seeded Fetch fixture.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"time"
)

const count = 512

func mix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
func record(offset uint64) ([]byte, []byte) {
	seed := uint64(0x5eed0001)
	hash := mix(seed*uint64(0x9e3779b97f4a7c15) + offset)
	key, value := make([]byte, 16), make([]byte, 100)
	binary.BigEndian.PutUint64(key[4:], offset)
	binary.BigEndian.PutUint32(key[12:], uint32(hash))
	binary.BigEndian.PutUint64(value, offset)
	binary.BigEndian.PutUint64(value[8:], hash)
	for at := 16; at < 100; at += 8 {
		hash = mix(hash)
		word := make([]byte, 8)
		binary.BigEndian.PutUint64(word, hash)
		copy(value[at:], word)
	}
	return key, value
}
func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("one bootstrap address required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	p, err := kgo.NewClient(kgo.SeedBrokers(os.Args[1]), kgo.ClientID("nullbroker-qualification"), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.DisableIdempotentWrite(), kgo.ProducerLinger(0), kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.RecordDeliveryTimeout(4*time.Second), kgo.RequestTimeoutOverhead(3*time.Second), kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	if err != nil {
		return err
	}
	acknowledged := 0
	records := make([]*kgo.Record, count)
	for i := range records {
		key, value := record(uint64(i))
		records[i] = &kgo.Record{Topic: "nullbroker-peer", Partition: 0, Timestamp: time.UnixMilli(0), Key: key, Value: value}
	}
	if err := p.ProduceSync(ctx, records...).FirstErr(); err != nil {
		p.Close()
		return err
	}
	for i, r := range records {
		if r.Partition != 0 || r.Offset != int64(i) {
			p.Close()
			return fmt.Errorf("delivery offset")
		}
		acknowledged++
	}
	p.Close()
	c, err := kgo.NewClient(kgo.SeedBrokers(os.Args[1]), kgo.ClientID("nullbroker-qualification"), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"nullbroker-peer": {0: kgo.NewOffset().At(0)}}), kgo.FetchMinBytes(1), kgo.FetchMaxWait(10*time.Millisecond), kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	if err != nil {
		return err
	}
	defer c.Close()
	fetched := 0
	for fetched < count {
		fetches := c.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("fetch errors: %v", errs)
		}
		for _, r := range fetches.Records() {
			key, value := record(uint64(fetched))
			if r.Partition != 0 || r.Offset != int64(fetched) || r.Timestamp.UnixMilli() != 0 || len(r.Headers) != 0 || !bytes.Equal(r.Key, key) || !bytes.Equal(r.Value, value) {
				return fmt.Errorf("fetch validation %d", fetched)
			}
			fetched++
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"peer": "franz-go", "version": "1.22.0", "acknowledged": acknowledged, "validated_fetch": fetched, "validation_failures": 0, "fetch_source": "seeded-independent-of-produce"})
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
