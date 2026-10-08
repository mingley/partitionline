// Command peer is the pinned franz-go benchmark peer driver (KL09-65).
//
// Subcommands (all env-configured, see peer.LoadConfig):
//
//	emit-config   print the resolved effective configuration as JSON (no broker)
//	scenarios     print the supported/unsupported scenario matrix (no broker)
//	produce       produce COUNT records, print an ack summary (needs broker)
//	fetch         consume and verify records, print a verify summary (needs broker)
//	roundtrip     warmup + steady produce + consume-verify + result file (needs broker)
package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	peer "github.com/mingley/partitionline/benchmarks/peers/franz-go"
)

func fail(err error) int {
	fmt.Fprintf(os.Stderr, "franzgo-peer: %v\n", err)
	return 1
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: peer <emit-config|scenarios|produce|fetch|roundtrip>")
		return 2
	}
	switch args[0] {
	case "emit-config":
		return emitConfig()
	case "scenarios":
		return scenarios()
	case "produce":
		return produce()
	case "fetch":
		return fetch()
	case "roundtrip":
		return roundtrip()
	default:
		fmt.Fprintf(os.Stderr, "usage: peer <emit-config|scenarios|produce|fetch|roundtrip>\n")
		return 2
	}
}

func emitConfig() int {
	cfg, err := peer.LoadConfig()
	if err != nil {
		return fail(err)
	}
	out, _ := json.MarshalIndent(cfg.EffectiveSettings(), "", " ")
	fmt.Println(string(out))
	return 0
}

func scenarios() int {
	out, _ := json.MarshalIndent(peer.SupportedScenarios(), "", " ")
	fmt.Println(string(out))
	return 0
}

// clientOpts translates the resolved config into franz-go options.
// Every knob here is covered by the emitted effective settings.
func clientOpts(cfg peer.Config, tlsCfg *tls.Config) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Bootstrap...),
		kgo.ClientID("franzgo-peer"),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
	}
	switch cfg.Acks {
	case -1:
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case 0:
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	default:
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
	}
	if !cfg.Idempotent {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	opts = append(opts,
		kgo.ProducerLinger(time.Duration(cfg.LingerMs)*time.Millisecond),
		kgo.MaxProduceRequestsInflightPerBroker(cfg.MaxInFlight),
	)
	if cfg.BatchBytes > 0 && cfg.BatchBytes <= math.MaxInt32 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(int32(cfg.BatchBytes)))
	}
	// Client-side buffers are sized to the run so backpressure never
	// rejects: the contract counts broker acks, not queue admissions.
	bufRecords := cfg.Count + cfg.Warmup + 1024
	if bufRecords > math.MaxInt32 {
		bufRecords = math.MaxInt32
	}
	opts = append(opts, kgo.MaxBufferedRecords(int(bufRecords)))
	switch cfg.Compression {
	case "none":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
	case "gzip":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
	case "snappy":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
	case "lz4":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
	case "zstd":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
	}
	if tlsCfg != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	authFn := func(context.Context) (plain.Auth, error) {
		return plain.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}, nil
	}
	scramFn := func(context.Context) (scram.Auth, error) {
		return scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}, nil
	}
	switch cfg.SASLMechanism {
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Plain(authFn)))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Sha256(scramFn)))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Sha512(scramFn)))
	}
	return opts, nil
}

type produceOutcome struct {
	Offered  int64
	Accepted int64
	Acked    int64
	Rejected int64
	TimedOut int64
	Unknown  int64
	Errors   []peer.ErrorEvent
	LatUs    []float64
	Bytes    int64
	Elapsed  time.Duration
}

// produceRecords produces count records to topic and accounts every
// record. latUs is filled only when wantLatency is true. Only broker
// promise results count as acknowledged.
func produceRecords(client *kgo.Client, cfg peer.Config, topic string, count uint64, wantLatency bool) produceOutcome {
	var out produceOutcome
	out.Offered = int64(count)
	if count == 0 {
		return out
	}
	var latUs []float64
	var starts []int64
	if wantLatency {
		latUs = make([]float64, count)
		starts = make([]int64, count)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	var acked atomic.Int64
	var mu sync.Mutex
	errCounts := map[string]int64{}
	start := time.Now()
	for i := uint64(0); i < count; i++ {
		rec := &kgo.Record{
			Topic:     topic,
			Key:       peer.MakeKey(i, cfg.Seed),
			Value:     peer.MakeValue(i, cfg.Seed, cfg.PayloadBytes),
			Timestamp: time.Now(),
		}
		out.Bytes += int64(len(rec.Key) + len(rec.Value))
		if wantLatency {
			starts[i] = time.Now().UnixNano()
		}
		idx := i
		client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			if err == nil {
				acked.Add(1)
				if wantLatency {
					latUs[idx] = float64(time.Now().UnixNano()-starts[idx]) / 1000.0
				}
				return
			}
			mu.Lock()
			errCounts[err.Error()]++
			mu.Unlock()
		})
	}
	out.Accepted = int64(count)
	flushErr := client.Flush(ctx)
	out.Elapsed = time.Since(start)
	out.Acked = acked.Load()
	if flushErr != nil {
		mu.Lock()
		errCounts["flush: "+flushErr.Error()]++
		mu.Unlock()
	}
	// Anything neither acked nor errored after a clean flush is unknown
	// (should be zero; franz-go resolves every promise on flush).
	accounted := out.Acked
	for _, n := range errCounts {
		accounted += n
	}
	if missing := out.Offered - accounted; missing > 0 {
		out.Unknown = missing
	}
	var names []string
	for name := range errCounts {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		out.Errors = append(out.Errors, peer.ErrorEvent{
			Code: "produce", Name: name, Count: errCounts[name], Fatal: false, Phase: "steady_state",
		})
		out.Rejected += errCounts[name]
	}
	out.LatUs = latUs
	return out
}

func produce() int {
	cfg, err := peer.LoadConfig()
	if err != nil {
		return fail(err)
	}
	tlsCfg, err := cfg.TLSConfig()
	if err != nil {
		return fail(err)
	}
	opts, err := clientOpts(cfg, tlsCfg)
	if err != nil {
		return fail(err)
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fail(fmt.Errorf("franz-go client: %w", err))
	}
	defer client.Close()
	out := produceRecords(client, cfg, cfg.Topic, cfg.Count, false)
	summary := map[string]any{
		"offered":     out.Offered,
		"accepted":    out.Accepted,
		"acked":       out.Acked,
		"rejected":    out.Rejected,
		"unknown":     out.Unknown,
		"elapsed_s":   out.Elapsed.Seconds(),
		"acked_rec_s": float64(out.Acked) / out.Elapsed.Seconds(),
		"errors":      out.Errors,
	}
	enc, _ := json.Marshal(summary)
	fmt.Println(string(enc))
	if out.Acked != out.Offered {
		return fail(fmt.Errorf("acked %d of %d offered", out.Acked, out.Offered))
	}
	return 0
}

type fetchOutcome struct {
	Consumed   int64
	Verified   int64
	Missing    int64
	Duplicates int64
	BadPayload int64
	Bytes      int64
	Elapsed    time.Duration
	PerPartMax map[int32]int64
}

func fetchRecords(client *kgo.Client, cfg peer.Config, expect uint64) fetchOutcome {
	var out fetchOutcome
	out.PerPartMax = map[int32]int64{}
	seen := make([]bool, expect)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	start := time.Now()
	idleDeadline := time.Now().Add(15 * time.Second)
	for {
		if out.Verified >= int64(expect) {
			break
		}
		if time.Now().After(idleDeadline) {
			break
		}
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		empty := true
		fetches.EachRecord(func(r *kgo.Record) {
			empty = false
			out.Consumed++
			out.Bytes += int64(len(r.Key) + len(r.Value))
			if r.Offset > out.PerPartMax[r.Partition] {
				out.PerPartMax[r.Partition] = r.Offset
			}
			idx, err := peer.Verify(r.Key, r.Value, cfg.Seed, cfg.PayloadBytes)
			if err != nil {
				out.BadPayload++
				return
			}
			if idx >= expect {
				out.BadPayload++
				return
			}
			if seen[idx] {
				out.Duplicates++
				return
			}
			seen[idx] = true
			out.Verified++
		})
		if !empty {
			idleDeadline = time.Now().Add(15 * time.Second)
		}
	}
	out.Elapsed = time.Since(start)
	for _, s := range seen {
		if !s {
			out.Missing++
		}
	}
	return out
}

func fetch() int {
	cfg, err := peer.LoadConfig()
	if err != nil {
		return fail(err)
	}
	tlsCfg, err := cfg.TLSConfig()
	if err != nil {
		return fail(err)
	}
	opts, err := clientOpts(cfg, tlsCfg)
	if err != nil {
		return fail(err)
	}
	opts = append(opts,
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(time.Second),
	)
	if cfg.Isolation == "read_committed" {
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	} else {
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadUncommitted()))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fail(fmt.Errorf("franz-go client: %w", err))
	}
	defer client.Close()
	out := fetchRecords(client, cfg, cfg.Count)
	summary := map[string]any{
		"consumed":   out.Consumed,
		"verified":   out.Verified,
		"missing":    out.Missing,
		"duplicates": out.Duplicates,
		"bad":        out.BadPayload,
		"elapsed_s":  out.Elapsed.Seconds(),
	}
	enc, _ := json.Marshal(summary)
	fmt.Println(string(enc))
	if out.Missing != 0 || out.BadPayload != 0 {
		return fail(fmt.Errorf("missing=%d bad=%d duplicates=%d", out.Missing, out.BadPayload, out.Duplicates))
	}
	return 0
}

func roundtrip() int {
	cfg, err := peer.LoadConfig()
	if err != nil {
		return fail(err)
	}
	runStart := time.Now()
	tlsCfg, err := cfg.TLSConfig()
	if err != nil {
		return fail(err)
	}
	opts, err := clientOpts(cfg, tlsCfg)
	if err != nil {
		return fail(err)
	}
	adminClient, err := kgo.NewClient(opts...)
	if err != nil {
		return fail(fmt.Errorf("franz-go client: %w", err))
	}
	defer adminClient.Close()
	adm := kadm.NewClient(adminClient)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	warmupTopic := cfg.Topic + "-warmup"
	// Fresh topics per run (contract section 4.7): stale records from
	// any prior attempt would corrupt the ID and high-watermark audits.
	for _, topic := range []string{cfg.Topic, warmupTopic} {
		if err := recreateTopic(adm, ctx, topic, int32(cfg.Partitions)); err != nil {
			return fail(err)
		}
	}
	startOffsets, err := endOffsets(adm, ctx, cfg.Topic)
	if err != nil {
		return fail(fmt.Errorf("start offsets: %w", err))
	}

	// Warmup phase: records go to a separate topic so the high-watermark
	// audit on the steady-state topic stays exact.
	warmStart := time.Now()
	warmOut := produceRecords(adminClient, cfg, warmupTopic, cfg.Warmup, false)
	warmElapsed := time.Since(warmStart)
	warmOK := warmOut.Acked == int64(cfg.Warmup)

	resStart := peer.SnapshotResources()
	steadyStart := time.Now()
	steady := produceRecords(adminClient, cfg, cfg.Topic, cfg.Count, true)
	steadyElapsed := time.Since(steadyStart)

	endOffsetsMap, err := endOffsets(adm, ctx, cfg.Topic)
	if err != nil {
		return fail(fmt.Errorf("end offsets: %w", err))
	}

	fetchOpts := append(append([]kgo.Opt(nil), opts...),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(time.Second),
	)
	if cfg.Isolation == "read_committed" {
		fetchOpts = append(fetchOpts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	} else {
		fetchOpts = append(fetchOpts, kgo.FetchIsolationLevel(kgo.ReadUncommitted()))
	}
	fetchClient, err := kgo.NewClient(fetchOpts...)
	if err != nil {
		return fail(fmt.Errorf("franz-go fetch client: %w", err))
	}
	defer fetchClient.Close()
	fetched := fetchRecords(fetchClient, cfg, cfg.Count)
	resEnd := peer.SnapshotResources()
	runEnd := time.Now()

	doc, docErr := buildResult(cfg, runStart, runEnd, warmElapsed, warmOK, &steady, steadyElapsed, &fetched, startOffsets, endOffsetsMap, resStart, resEnd, adm, ctx)
	if docErr != nil {
		return fail(docErr)
	}
	data, err := json.MarshalIndent(doc, "", " ")
	if err != nil {
		return fail(err)
	}
	if err := os.WriteFile(cfg.ResultPath, append(data, '\n'), 0644); err != nil {
		return fail(err)
	}
	fmt.Printf("roundtrip: acked=%d consumed=%d verified=%d missing=%d result=%s\n",
		steady.Acked, fetched.Consumed, fetched.Verified, fetched.Missing, cfg.ResultPath)
	if doc.Integrity.IntegrityFailure {
		return fail(fmt.Errorf("integrity failure (see %s)", cfg.ResultPath))
	}
	return 0
}

// recreateTopic deletes topic if present, waits for the deletion to
// settle, then creates it with partitions (RF 1, isolated broker).
func recreateTopic(adm *kadm.Client, ctx context.Context, topic string, partitions int32) error {
	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		return fmt.Errorf("delete topic %s: %w", topic, err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		md, err := adm.Metadata(ctx, topic)
		if err != nil {
			return fmt.Errorf("metadata %s: %w", topic, err)
		}
		if td, ok := md.Topics[topic]; !ok || len(td.Partitions) == 0 {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("topic %s still present 60s after delete", topic)
		}
		time.Sleep(500 * time.Millisecond)
	}
	resps, err := adm.CreateTopics(ctx, partitions, 1, nil, topic)
	if err != nil {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	for _, resp := range resps {
		if resp.Err != nil {
			return fmt.Errorf("create topic %s: %w", topic, resp.Err)
		}
	}
	return nil
}

func endOffsets(adm *kadm.Client, ctx context.Context, topic string) (map[int32]int64, error) {
	listed, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	out := map[int32]int64{}
	for _, offsets := range listed {
		for partition, listedOffset := range offsets {
			if listedOffset.Err != nil {
				return nil, listedOffset.Err
			}
			out[partition] = listedOffset.Offset
		}
	}
	return out, nil
}

func buildResult(cfg peer.Config, runStart, runEnd time.Time, warmElapsed time.Duration, warmOK bool,
	steady *produceOutcome, steadyElapsed time.Duration, fetched *fetchOutcome,
	startOffsets, endOffsets map[int32]int64, resStart, resEnd peer.ResourceSnapshot,
	adm *kadm.Client, ctx context.Context) (*peer.ResultDoc, error) {

	// High-watermark audit over the union of observed partitions.
	partSet := map[int32]bool{}
	for p := range startOffsets {
		partSet[p] = true
	}
	for p := range endOffsets {
		partSet[p] = true
	}
	var partitions []int32
	for p := range partSet {
		partitions = append(partitions, p)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	var hwParts []peer.PartitionHW
	var totalDelta int64
	for _, p := range partitions {
		delta := endOffsets[p] - startOffsets[p]
		totalDelta += delta
		hwParts = append(hwParts, peer.PartitionHW{
			Partition: int(p), StartOffset: startOffsets[p], EndOffset: endOffsets[p], OffsetDelta: delta,
		})
	}
	hwMatches := totalDelta == steady.Acked
	payloadOK := fetched.BadPayload == 0 && fetched.Missing == 0
	integrityOK := hwMatches && payloadOK && fetched.Duplicates == 0 && steady.Rejected == 0 && steady.Unknown == 0 && warmOK
	verdict := "executed"
	attemptStatus := "passed_measurement"
	var attemptErr *string
	if !integrityOK {
		verdict = "failed"
		attemptStatus = "failed_integrity"
		msg := fmt.Sprintf("hw_match=%v missing=%d bad=%d dup=%d rejected=%d unknown=%d warmup_ok=%v",
			hwMatches, fetched.Missing, fetched.BadPayload, fetched.Duplicates, steady.Rejected, steady.Unknown, warmOK)
		attemptErr = &msg
	}

	latency := peer.LatencyStats(steady.LatUs)
	wallS := steadyElapsed.Seconds()
	recS := float64(steady.Acked) / wallS
	mbS := float64(steady.Bytes) / 1e6 / wallS
	userS := resEnd.UserSeconds - resStart.UserSeconds
	sysS := resEnd.SystemSeconds - resStart.SystemSeconds
	cores := float64(runtime.NumCPU())
	cpuPct := 0.0
	if wallS > 0 && cores > 0 {
		cpuPct = (userS + sysS) / wallS / cores * 100
	}

	binPath, binSHA, err := peer.SelfSHA256()
	if err != nil {
		return nil, err
	}
	effJSON, _ := json.MarshalIndent(cfg.EffectiveSettings(), "", " ")
	effPath := strings.TrimSuffix(cfg.ResultPath, ".json") + ".effective-config.json"
	if err := os.WriteFile(effPath, append(effJSON, '\n'), 0644); err != nil {
		return nil, err
	}
	effInfo, _ := os.Stat(effPath)

	commit, branch, clean, tree := peer.GitProvenance()
	franzgo, kadmV := peer.BuildVersions()
	clusterID := os.Getenv("CLUSTER_ID")
	brokerNodes := len(cfg.Bootstrap)
	if md, err := adm.Metadata(ctx); err == nil {
		if md.Cluster != "" {
			clusterID = md.Cluster
		}
		if len(md.Brokers) > 0 {
			brokerNodes = len(md.Brokers)
		}
	}
	if clusterID == "" {
		clusterID = "unknown"
	}

	doc := &peer.ResultDoc{
		SchemaVersion:   "1.0.0",
		ContractVersion: "1.1.0",
		SuiteHold: peer.SuiteHold{
			Status: "active",
			Policy: "Suite HOLD remains active; this result file is not a scenario pass.",
			Note:   "Peer driver validation only (KL09-65); not a cell pass.",
		},
		Scenario: peer.Scenario{
			ScenarioID:      cfg.ScenarioID,
			Profile:         cfg.Profile,
			Tier:            "exploratory",
			Peer:            "peer-adapter",
			CellDisposition: verdict,
			EqualSemantics: peer.EqualSemantics{
				Durability:  peer.Durability{ReplicationFactor: 1, MinInsyncReplicas: 1},
				Acks:        cfg.Acks,
				Idempotence: cfg.Idempotent,
				Isolation:   cfg.Isolation,
				Security:    peer.Security{Protocol: cfg.SecurityProtocol(), SASLMechanism: cfg.SASLMechanism},
			},
		},
		Provenance: peer.Provenance{
			Source: peer.Source{
				GitCommit: commit, GitBranch: branch,
				RepoURL: "https://github.com/mingley/partitionline",
				Clean:   clean, TreeHash: tree,
				Note: "concrete peer: franz-go (result-schema peer-adapter until promoted)",
			},
			Binary: peer.Binary{Name: "peer", Path: binPath, SHA256: binSHA},
			Config: peer.ConfigProv{Path: effPath, SHA256: effFileSHA(effPath), EffectiveSettings: cfg.EffectiveSettings()},
			Toolchains: peer.Toolchains{
				Compiler: runtime.Version(), Runtime: runtime.Version(),
				BuildTool: "go build (go.mod toolchain pin)",
				FranzGo:   franzgo, Kadm: kadmV,
			},
			Broker: peer.BrokerProv{
				Image:   firstNonEmpty(os.Getenv("BROKER_IMAGE"), "apache/kafka"),
				Version: firstNonEmpty(os.Getenv("BROKER_VERSION"), "4.1.0"),
				Mode:    "kraft", ClusterID: clusterID, NodeCount: brokerNodes, Endpoints: cfg.Bootstrap,
			},
			Host: peer.HostProbe(),
			Topology: peer.Topology{
				Environment: "loopback", RTTMs: 0.1, RTTUnit: "milliseconds",
				ClientNodes: 1, BrokerNodes: brokerNodes, NetworkInterface: "127.0.0.1",
			},
			Timestamps: peer.Timestamps{
				StartUTC: peer.FormatTime(runStart), EndUTC: peer.FormatTime(runEnd),
				DurationSeconds: runEnd.Sub(runStart).Seconds(), DurationUnit: "seconds",
			},
			Seeds: peer.Seeds{
				PayloadSeed: fmt.Sprintf("0x%X", cfg.Seed), KeySeed: fmt.Sprintf("0x%X", cfg.Seed),
				PartitionSeed: "round_robin", RepetitionSeed: "single",
			},
			Artifacts: []peer.Artifact{{
				Path: effPath, Type: "effective-config", SHA256: effFileSHA(effPath), SizeBytes: effInfo.Size(),
			}},
		},
		Execution: peer.Execution{
			Phase: "steady_state", WarmupCompleted: warmOK && cfg.Warmup > 0,
			WarmupRecords: cfg.Warmup, WarmupDurationSeconds: warmElapsed.Seconds(),
			SteadyDurationSeconds: wallS, RepetitionIndex: 1, TotalRepetitions: 1,
			PairingOrder:             "single (peer driver validation, not a paired campaign)",
			CoordinatedOmissionAvoid: peer.Coordinated{Enabled: false, ScheduleType: "closed_loop"},
		},
		Outcomes: peer.Outcomes{
			Offered: steady.Offered, Accepted: steady.Accepted, Acknowledged: steady.Acked,
			Consumed: fetched.Verified, Rejected: steady.Rejected, TimedOut: steady.TimedOut, Unknown: steady.Unknown,
		},
		Measurements: peer.Measurements{
			Throughput: peer.Throughput{
				RecordsPerSecond: recS, RecordsUnit: "records/s",
				MBPerSecond: mbS, MBUnit: "MB/s",
				TotalBytes: steady.Bytes, TotalBytesUnit: "bytes", MBDefinition: "1 MB = 1e6 bytes",
			},
			Latency: latency,
			ClientResources: peer.ClientResources{
				CPUUtilizationPct: cpuPct, CPUUnit: "percent",
				UserCPUSeconds: math.Max(userS, 0), SystemCPUSeconds: math.Max(sysS, 0), CPUSecondsUnit: "seconds",
				Allocations: peer.Allocations{
					TotalAllocatedBytes: int64(resEnd.TotalAlloc - resStart.TotalAlloc),
					AllocationCount:     int64(resEnd.Mallocs - resStart.Mallocs), Unit: "bytes",
				},
				RSS: peer.RSS{
					PeakRSSBytes:    resEnd.PeakRSSBytes,
					AverageRSSBytes: (resStart.PeakRSSBytes + resEnd.PeakRSSBytes) / 2,
					Unit:            "bytes",
				},
				ThreadsCount: runtime.GOMAXPROCS(0),
				ThreadsNote:  "Go runtime: GOMAXPROCS cap; goroutines multiplex OS threads",
			},
			BrokerResources: peer.BrokerResources{
				CPUUtilizationPct: 0, CPUUnit: "percent", PeakRSSBytes: 0, RSSUnit: "bytes",
				DiskWriteBytes: 0, DiskWriteUnit: "bytes",
				Note: "unmeasured by this peer driver (external broker process)",
			},
			Errors: append([]peer.ErrorEvent(nil), steady.Errors...),
		},
		Integrity: peer.Integrity{
			Verified: integrityOK,
			RecordIDs: peer.RecordIDs{
				StartID: 0, EndID: int64(cfg.Count) - 1,
				ExpectedCount: int64(cfg.Count), VerifiedCount: fetched.Verified,
				MissingIDsCount: fetched.Missing, DuplicateIDsCount: fetched.Duplicates,
				ChecksumAlgorithm: "sha256", PayloadChecksumMatch: payloadOK,
			},
			HighWatermarkAudit: peer.HighWatermark{
				Partitions: hwParts, TotalOffsetDelta: totalDelta, MatchesAcknowledged: hwMatches,
			},
			IdempotenceSequenceVerified: fetched.Duplicates == 0 && fetched.Missing == 0,
			IntegrityFailure:            !integrityOK,
		},
		RepetitionHist: peer.RepetitionHist{
			TotalAttempts: 1, FailedAttempts: map[bool]int{true: 1, false: 0}[!integrityOK],
			Attempts: []peer.Attempt{{
				AttemptNumber: 1, RepetitionIndex: 1, Status: attemptStatus,
				IntegrityFailure: !integrityOK, ErrorMessage: attemptErr,
				TimestampUTC: peer.FormatTime(runEnd),
			}},
		},
	}
	if doc.Measurements.Errors == nil {
		doc.Measurements.Errors = []peer.ErrorEvent{}
	}
	return doc, nil
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

func sha256Sum(b []byte) []byte {
	h := sha256.New()
	h.Write(b)
	return h.Sum(nil)
}

func effFileSHA(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return strings.Repeat("0", 64)
	}
	return fmt.Sprintf("%x", sha256Sum(data))
}
