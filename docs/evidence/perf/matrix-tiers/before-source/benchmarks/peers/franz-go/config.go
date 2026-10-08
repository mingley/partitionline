package peer

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config is the resolved, validated peer configuration. Every knob
// comes from the environment (same names as the partitionline bench
// examples where they overlap); LoadConfig fails closed on unknown
// or out-of-range values.
type Config struct {
	Bootstrap  []string
	Topic      string
	Partitions int

	Count        uint64
	Warmup       uint64
	PayloadBytes int
	Seed         uint64

	Acks         int
	Idempotent   bool
	LingerMs     int64
	BatchBytes   int64
	BatchRecords int64
	MaxInFlight  int
	Compression  string
	Isolation    string

	TLSCAPem       []byte
	TLSServerName  string
	TLSClientCert  []byte
	TLSClientKey   []byte
	SASLMechanism  string
	SASLUsername   string
	SASLPassword   string
	HasCredentials bool

	ScenarioID string
	Profile    string
	ResultPath string
}

// SecurityProtocol derives the contract security protocol name from
// the configured transport/auth.
func (c Config) SecurityProtocol() string {
	tlsOn := len(c.TLSCAPem) > 0 || c.TLSServerName != "" || len(c.TLSClientCert) > 0
	saslOn := c.SASLMechanism != ""
	switch {
	case tlsOn && saslOn:
		return "SASL_SSL"
	case tlsOn:
		return "SSL"
	case saslOn:
		return "SASL_PLAINTEXT"
	default:
		return "PLAINTEXT"
	}
}

func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getInt(key string, def int64) (int64, error) {
	s, ok := os.LookupEnv(key)
	if !ok || s == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("peer: %s must be an integer, got %q", key, s)
	}
	return v, nil
}

func getUint(key string, def uint64) (uint64, error) {
	v, err := getInt(key, int64(def))
	if err != nil {
		return 0, err
	}
	if v < 0 {
		return 0, fmt.Errorf("peer: %s must be >= 0, got %d", key, v)
	}
	return uint64(v), nil
}

func readFileEnv(key string) ([]byte, error) {
	path, ok := os.LookupEnv(key)
	if !ok || path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("peer: read %s %s: %w", key, path, err)
	}
	return data, nil
}

// LoadConfig resolves every knob from the environment.
func LoadConfig() (Config, error) {
	var c Config
	var err error
	bootstrap := getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
	for _, b := range strings.Split(bootstrap, ",") {
		if b = strings.TrimSpace(b); b != "" {
			c.Bootstrap = append(c.Bootstrap, b)
		}
	}
	if len(c.Bootstrap) == 0 {
		return c, fmt.Errorf("peer: KAFKA_BOOTSTRAP must list at least one broker")
	}
	c.Topic = getenv("KAFKA_TOPIC", "partitionline")
	if c.Topic == "" {
		return c, fmt.Errorf("peer: KAFKA_TOPIC must not be empty")
	}
	var v int64
	if v, err = getInt("PARTITIONS", 6); err != nil {
		return c, err
	}
	c.Partitions = int(v)
	if c.Count, err = getUint("COUNT", 100000); err != nil {
		return c, err
	}
	if c.Warmup, err = getUint("WARMUP", 10000); err != nil {
		return c, err
	}
	if v, err = getInt("PAYLOAD_BYTES", 100); err != nil {
		return c, err
	}
	c.PayloadBytes = int(v)
	seedStr := getenv("RECORD_SEED", "0x5EED0001")
	seed, err := strconv.ParseUint(strings.TrimSpace(seedStr), 0, 64)
	if err != nil {
		return c, fmt.Errorf("peer: RECORD_SEED must be an integer, got %q", seedStr)
	}
	c.Seed = seed
	if v, err = getInt("ACKS", 1); err != nil {
		return c, err
	}
	c.Acks = int(v)
	c.Idempotent = getenv("IDEMPOTENT", "") == "1"
	if c.LingerMs, err = getInt("LINGER_MS", 5); err != nil {
		return c, err
	}
	if c.BatchBytes, err = getInt("BATCH_BYTES", 1048576); err != nil {
		return c, err
	}
	if c.BatchRecords, err = getInt("BATCH_RECORDS", 32768); err != nil {
		return c, err
	}
	if v, err = getInt("MAX_IN_FLIGHT", 5); err != nil {
		return c, err
	}
	c.MaxInFlight = int(v)
	c.Compression = strings.ToLower(strings.TrimSpace(getenv("COMPRESSION", "none")))
	c.Isolation = strings.ToLower(strings.TrimSpace(getenv("ISOLATION", "read_uncommitted")))
	if c.TLSCAPem, err = readFileEnv("TLS_CA_PEM"); err != nil {
		return c, err
	}
	c.TLSServerName = strings.TrimSpace(getenv("TLS_SERVER_NAME", ""))
	if c.TLSClientCert, err = readFileEnv("TLS_CLIENT_CERT_PEM"); err != nil {
		return c, err
	}
	if c.TLSClientKey, err = readFileEnv("TLS_CLIENT_KEY_PEM"); err != nil {
		return c, err
	}
	c.SASLMechanism = strings.ToUpper(strings.TrimSpace(getenv("SASL_MECHANISM", "")))
	c.SASLUsername = getenv("SASL_USERNAME", "")
	c.SASLPassword = getenv("SASL_PASSWORD", "")
	c.HasCredentials = c.SASLUsername != "" || c.SASLPassword != "" || len(c.TLSClientKey) > 0
	c.ScenarioID = getenv("SCENARIO_ID", "peer-roundtrip-plain-6p")
	c.Profile = strings.ToLower(strings.TrimSpace(getenv("PROFILE", "bulk")))
	c.ResultPath = getenv("RESULT_PATH", "franzgo-result.json")
	return c, c.Validate()
}

// Validate enforces fail-closed contract rules on a resolved config.
func (c Config) Validate() error {
	if len(c.Bootstrap) == 0 {
		return fmt.Errorf("peer: at least one bootstrap broker is required")
	}
	if c.Topic == "" {
		return fmt.Errorf("peer: topic must not be empty")
	}
	if c.Partitions < 1 {
		return fmt.Errorf("peer: PARTITIONS must be >= 1, got %d", c.Partitions)
	}
	if c.Count == 0 {
		return fmt.Errorf("peer: COUNT must be >= 1, got %d", c.Count)
	}
	if c.PayloadBytes < 0 {
		return fmt.Errorf("peer: PAYLOAD_BYTES must be >= 0, got %d", c.PayloadBytes)
	}
	switch c.Acks {
	case -1, 0, 1:
	default:
		return fmt.Errorf("peer: ACKS must be -1, 0 or 1, got %d", c.Acks)
	}
	if c.Idempotent && c.MaxInFlight > 5 {
		return fmt.Errorf("peer: idempotent produce requires MAX_IN_FLIGHT <= 5, got %d", c.MaxInFlight)
	}
	if c.MaxInFlight < 1 {
		return fmt.Errorf("peer: MAX_IN_FLIGHT must be >= 1, got %d", c.MaxInFlight)
	}
	if c.LingerMs < 0 {
		return fmt.Errorf("peer: LINGER_MS must be >= 0, got %d", c.LingerMs)
	}
	if c.BatchBytes < 0 || c.BatchRecords < 0 {
		return fmt.Errorf("peer: BATCH_BYTES/BATCH_RECORDS must be >= 0")
	}
	switch c.Compression {
	case "none", "gzip", "snappy", "lz4", "zstd":
	default:
		return fmt.Errorf("peer: COMPRESSION must be none|gzip|snappy|lz4|zstd, got %q", c.Compression)
	}
	switch c.Isolation {
	case "read_uncommitted", "read_committed":
	default:
		return fmt.Errorf("peer: ISOLATION must be read_uncommitted|read_committed, got %q", c.Isolation)
	}
	switch c.SASLMechanism {
	case "", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
	default:
		return fmt.Errorf("peer: SASL_MECHANISM %q is unsupported (see scenarios)", c.SASLMechanism)
	}
	if c.SASLMechanism != "" && (c.SASLUsername == "" || c.SASLPassword == "") {
		return fmt.Errorf("peer: SASL %s requires SASL_USERNAME and SASL_PASSWORD", c.SASLMechanism)
	}
	if (len(c.TLSClientCert) > 0) != (len(c.TLSClientKey) > 0) {
		return fmt.Errorf("peer: mTLS needs both TLS_CLIENT_CERT_PEM and TLS_CLIENT_KEY_PEM")
	}
	switch c.Profile {
	case "low-latency", "bulk", "fetch", "transactional", "group/share", "secure":
	default:
		return fmt.Errorf("peer: PROFILE must be a contract profile, got %q", c.Profile)
	}
	if c.ResultPath == "" {
		return fmt.Errorf("peer: RESULT_PATH must not be empty")
	}
	return nil
}

// EffectiveSettings returns the resolved configuration exactly as it
// will drive franz-go, for provenance emission. Secrets are never
// included (presence flags only).
func (c Config) EffectiveSettings() map[string]any {
	return map[string]any{
		"acks":                  c.Acks,
		"linger_ms":             c.LingerMs,
		"batch_size_bytes":      c.BatchBytes,
		"batch_num_messages":    c.BatchRecords,
		"max_in_flight":         c.MaxInFlight,
		"idempotence":           c.Idempotent,
		"compression":           c.Compression,
		"isolation_level":       c.Isolation,
		"security_protocol":     c.SecurityProtocol(),
		"sasl_mechanism":        c.SASLMechanism,
		"has_sasl_credentials":  c.SASLUsername != "" && c.SASLPassword != "",
		"has_custom_ca":         len(c.TLSCAPem) > 0,
		"tls_server_name":       c.TLSServerName,
		"has_client_identity":   len(c.TLSClientCert) > 0,
		"bootstrap":             c.Bootstrap,
		"topic":                 c.Topic,
		"partitions":            c.Partitions,
		"count":                 c.Count,
		"warmup":                c.Warmup,
		"payload_bytes":         c.PayloadBytes,
		"record_seed":           fmt.Sprintf("0x%X", c.Seed),
		"partitioner":           "round_robin",
		"franzgo_client_id":     "franzgo-peer",
		"delivery_definition":   "acknowledged = broker Produce response with nil promise error; enqueue is never counted",
		"record_id_definition":  "key = be64(index) || be64(splitmix64(seed^index)); value = splitmix64 stream; sha256 verified on consume",
		"unsupported_by_driver": UnsupportedIDs(),
	}
}

// TLSConfig builds the franz-go TLS dialer config, or nil for
// plaintext. Private CAs and mTLS identity come from the resolved PEMs.
func (c Config) TLSConfig() (*tls.Config, error) {
	if c.SecurityProtocol() != "SSL" && c.SecurityProtocol() != "SASL_SSL" {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12} // #nosec G402 -- floor only; brokers negotiate 1.3.
	if len(c.TLSCAPem) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(c.TLSCAPem) {
			return nil, fmt.Errorf("peer: TLS_CA_PEM parsed zero certificates")
		}
		cfg.RootCAs = pool
	}
	if len(c.TLSClientCert) > 0 {
		cert, err := tls.X509KeyPair(c.TLSClientCert, c.TLSClientKey)
		if err != nil {
			return nil, fmt.Errorf("peer: mTLS identity: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	if c.TLSServerName != "" {
		cfg.ServerName = c.TLSServerName
	}
	return cfg, nil
}

// ScenarioSupport reports whether this driver implements a contract
// scenario cell. Unsupported cells are explicit, never silent.
type ScenarioSupport struct {
	ID        string `json:"id"`
	Supported bool   `json:"supported"`
	Reason    string `json:"reason"`
}

// SupportedScenarios is the driver's explicit capability matrix.
func SupportedScenarios() []ScenarioSupport {
	return []ScenarioSupport{
		{ID: "bulk produce (acks -1/1/0, idempotent, compression, batching)", Supported: true, Reason: "produce/roundtrip commands"},
		{ID: "bulk fetch with ID/hash verification (both isolation levels)", Supported: true, Reason: "fetch/roundtrip commands"},
		{ID: "secure transport (SSL/SASL_SSL, PLAIN, SCRAM-SHA-256/512, mTLS)", Supported: true, Reason: "TLS/SASL env knobs"},
		{ID: "low-latency open-loop scheduler", Supported: false, Reason: "driver is closed-loop only; open-loop cells need a scheduler this driver does not implement"},
		{ID: "transactions (produce + read_committed filtering)", Supported: false, Reason: "driver does not implement transactional produce or abort filtering"},
		{ID: "KIP-848/KIP-932 group and share cells", Supported: false, Reason: "driver consumes by direct assignment; no group coordinator or share RPCs"},
		{ID: "OAUTHBEARER/OIDC refresh", Supported: false, Reason: "driver wires PLAIN and SCRAM only"},
	}
}

// UnsupportedIDs lists the matrix IDs the driver does not implement.
func UnsupportedIDs() []string {
	var out []string
	for _, s := range SupportedScenarios() {
		if !s.Supported {
			out = append(out, s.ID)
		}
	}
	return out
}
