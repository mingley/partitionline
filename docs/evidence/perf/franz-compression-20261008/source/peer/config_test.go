package peer

import (
	"encoding/json"
	"strings"
	"testing"
)

// setEnv pins the process environment for one offline config test.
func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	keys := []string{
		"KAFKA_BOOTSTRAP", "KAFKA_TOPIC", "PARTITIONS", "COUNT", "WARMUP",
		"PAYLOAD_BYTES", "RECORD_SEED", "ACKS", "IDEMPOTENT", "LINGER_MS",
		"BATCH_BYTES", "BATCH_RECORDS", "MAX_IN_FLIGHT", "COMPRESSION",
		"ISOLATION", "TLS_CA_PEM", "TLS_SERVER_NAME", "TLS_CLIENT_CERT_PEM",
		"TLS_CLIENT_KEY_PEM", "SASL_MECHANISM", "SASL_USERNAME", "SASL_PASSWORD",
		"SCENARIO_ID", "PROFILE", "RESULT_PATH",
	}
	for _, k := range keys {
		if _, ok := kv[k]; !ok {
			t.Setenv(k, "")
		}
	}
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	setEnv(t, map[string]string{"KAFKA_BOOTSTRAP": "127.0.0.1:9092"})
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("defaults: %v", err)
	}
	if len(cfg.Bootstrap) != 1 || cfg.Bootstrap[0] != "127.0.0.1:9092" {
		t.Fatalf("bootstrap: %v", cfg.Bootstrap)
	}
	if cfg.Topic != "partitionline" || cfg.Partitions != 6 {
		t.Fatalf("topic/partitions: %v %v", cfg.Topic, cfg.Partitions)
	}
	if cfg.Count != 100000 || cfg.Warmup != 10000 || cfg.PayloadBytes != 100 {
		t.Fatalf("workload: %+v", cfg)
	}
	if cfg.Seed != DefaultSeed {
		t.Fatalf("seed = %#x, want %#x", cfg.Seed, DefaultSeed)
	}
	if cfg.Acks != 1 || cfg.Idempotent || cfg.LingerMs != 5 {
		t.Fatalf("produce knobs: %+v", cfg)
	}
	if cfg.BatchBytes != 1048576 || cfg.BatchRecords != 32768 || cfg.MaxInFlight != 5 {
		t.Fatalf("batch knobs: %+v", cfg)
	}
	if cfg.SecurityProtocol() != "PLAINTEXT" {
		t.Fatalf("security = %s", cfg.SecurityProtocol())
	}
}

func TestLoadConfigRejects(t *testing.T) {
	cases := map[string]map[string]string{
		"bad acks":           {"ACKS": "2"},
		"bad compression":    {"COMPRESSION": "snazzy"},
		"bad isolation":      {"ISOLATION": "read_sometimes"},
		"bad profile":        {"PROFILE": "bulk-plus"},
		"bad mech":           {"SASL_MECHANISM": "OAUTHBEARER", "SASL_USERNAME": "u", "SASL_PASSWORD": "p"},
		"sasl without creds": {"SASL_MECHANISM": "PLAIN"},
		"zero count":         {"COUNT": "0"},
		"zero partitions":    {"PARTITIONS": "0"},
		"half mtls":          {"TLS_CLIENT_CERT_PEM": "/nonexistent"},
		"idempotent surge":   {"IDEMPOTENT": "1", "MAX_IN_FLIGHT": "6"},
	}
	for name, kv := range cases {
		setEnv(t, kv)
		if _, err := LoadConfig(); err == nil {
			t.Fatalf("%s: accepted, want rejection", name)
		}
	}
}

func TestEffectiveSettingsShape(t *testing.T) {
	setEnv(t, map[string]string{
		"ACKS": "-1", "IDEMPOTENT": "1", "COMPRESSION": "lz4",
		"SASL_MECHANISM": "SCRAM-SHA-256", "SASL_USERNAME": "alice", "SASL_PASSWORD": "s3cret-value",
	})
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.SecurityProtocol() != "SASL_PLAINTEXT" {
		t.Fatalf("security = %s", cfg.SecurityProtocol())
	}
	eff := cfg.EffectiveSettings()
	raw, _ := json.Marshal(eff)
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("effective settings not JSON: %v", err)
	}
	for _, key := range []string{"acks", "linger_ms", "batch_size_bytes", "max_in_flight", "idempotence",
		"compression", "security_protocol", "delivery_definition", "record_id_definition", "unsupported_by_driver"} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("effective settings missing %q", key)
		}
	}
	if decoded["acks"] != float64(-1) || decoded["idempotence"] != true {
		t.Fatalf("effective settings: %v", decoded)
	}
	// Secrets never appear, presence flags do.
	blob := string(raw)
	for _, secret := range []string{"alice", "s3cret-value"} {
		if strings.Contains(blob, secret) {
			t.Fatalf("secret leaked into effective settings: %s", blob)
		}
	}
	if decoded["has_sasl_credentials"] != true {
		t.Fatalf("presence flag: %v", decoded)
	}
}

func TestScenarioMatrixExplicit(t *testing.T) {
	matrix := SupportedScenarios()
	if len(matrix) == 0 {
		t.Fatal("empty scenario matrix")
	}
	var supported, unsupported int
	for _, s := range matrix {
		if s.ID == "" || s.Reason == "" {
			t.Fatalf("matrix entry missing id/reason: %+v", s)
		}
		if s.Supported {
			supported++
		} else {
			unsupported++
		}
	}
	if supported == 0 || unsupported == 0 {
		t.Fatalf("matrix must list both sides, got %+v", matrix)
	}
	if len(UnsupportedIDs()) != unsupported {
		t.Fatal("UnsupportedIDs disagrees with the matrix")
	}
}
