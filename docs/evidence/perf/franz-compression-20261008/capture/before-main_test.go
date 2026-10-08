package main

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	peer "github.com/mingley/partitionline/benchmarks/peers/franz-go"
)

func TestClientOptsCompressionMatchesEffectiveSettings(t *testing.T) {
	for _, key := range []string{
		"KAFKA_BOOTSTRAP", "KAFKA_TOPIC", "PARTITIONS", "COUNT", "WARMUP",
		"PAYLOAD_BYTES", "RECORD_SEED", "ACKS", "IDEMPOTENT", "LINGER_MS",
		"BATCH_BYTES", "BATCH_RECORDS", "MAX_IN_FLIGHT", "COMPRESSION",
		"ISOLATION", "TLS_CA_PEM", "TLS_SERVER_NAME", "TLS_CLIENT_CERT_PEM",
		"TLS_CLIENT_KEY_PEM", "SASL_MECHANISM", "SASL_USERNAME", "SASL_PASSWORD",
		"SCENARIO_ID", "PROFILE", "RESULT_PATH",
	} {
		t.Setenv(key, "")
	}
	cases := []struct {
		name string
		env  string
		want kgo.CompressionCodec
	}{
		{"default", "", kgo.NoCompression()},
		{"none", "none", kgo.NoCompression()},
		{"gzip", "gzip", kgo.GzipCompression()},
		{"snappy", "snappy", kgo.SnappyCompression()},
		{"lz4", "lz4", kgo.Lz4Compression()},
		{"zstd", "zstd", kgo.ZstdCompression()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("COMPRESSION", tc.env)
			cfg, err := peer.LoadConfig()
			if err != nil {
				t.Fatal(err)
			}
			wantName := tc.env
			if wantName == "" {
				wantName = "none"
			}
			if cfg.EffectiveSettings()["compression"] != wantName {
				t.Fatalf("reported compression differs from %q", wantName)
			}
			opts, err := clientOpts(cfg, nil)
			if err != nil {
				t.Fatal(err)
			}
			// Use the actual pinned SDK's resolved options. Block dialing so
			// the configuration check cannot contact a broker.
			opts = append(opts, kgo.Dialer(func(context.Context, string, string) (net.Conn, error) {
				return nil, errors.New("offline configuration test")
			}))
			client, err := kgo.NewClient(opts...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(client.Close)
			got := client.OptValue(kgo.ProducerBatchCompression)
			want := []kgo.CompressionCodec{tc.want}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("reports %q but SDK resolves compression %v; want %v", wantName, got, want)
			}
		})
	}
}
