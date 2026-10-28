package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	peer "github.com/mingley/partitionline/benchmarks/peers/franz-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTLSClientOptionsAndHandshake(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	tlsCfg := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	cfg, err := peer.LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	opts, err := clientOpts(cfg, tlsCfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatalf("TLS options rejected by SDK: %v", err)
	}
	client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	host := strings.TrimPrefix(server.URL, "https://")
	conn, err := dialBroker(ctx, "tcp", host, true, tlsCfg)
	if err != nil {
		t.Fatalf("trusted TLS handshake: %v", err)
	}
	conn.Close()
	badCfg := tlsCfg.Clone()
	badCfg.ServerName = "wrong.example"
	if conn, err := dialBroker(ctx, "tcp", host, true, badCfg); err == nil {
		conn.Close()
		t.Fatal("accepted a certificate for the wrong server name")
	}
}

func TestSuccessfulPromisesRequireBrokerAcknowledgements(t *testing.T) {
	for _, tc := range []struct {
		acks, acked, unknown int
	}{
		{0, 0, 3},
		{1, 3, 0},
		{-1, 3, 0},
	} {
		var out produceOutcome
		out.recordSuccessfulPromises(tc.acks, 3)
		if out.Acked != int64(tc.acked) || out.Unknown != int64(tc.unknown) {
			t.Fatalf("acks=%d: acknowledged=%d unknown=%d", tc.acks, out.Acked, out.Unknown)
		}
	}
}

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
