package peer

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// ResultDoc is the benchmark-report.py result document (schema
// benchmarks/result-schema.json, contract 1.1.0). Field names and
// units match the schema exactly.
type ResultDoc struct {
	SchemaVersion   string         `json:"schema_version"`
	ContractVersion string         `json:"contract_version"`
	SuiteHold       SuiteHold      `json:"suite_hold"`
	Scenario        Scenario       `json:"scenario"`
	Provenance      Provenance     `json:"provenance"`
	Execution       Execution      `json:"execution"`
	Outcomes        Outcomes       `json:"outcomes"`
	Measurements    Measurements   `json:"measurements"`
	Integrity       Integrity      `json:"integrity"`
	RepetitionHist  RepetitionHist `json:"repetition_history"`
}

type SuiteHold struct {
	Status string `json:"status"`
	Policy string `json:"policy"`
	Note   string `json:"note"`
}

type Scenario struct {
	ScenarioID      string         `json:"scenario_id"`
	Profile         string         `json:"profile"`
	Tier            string         `json:"tier"`
	Peer            string         `json:"peer"`
	CellDisposition string         `json:"cell_disposition"`
	EqualSemantics  EqualSemantics `json:"equal_semantics"`
}

type EqualSemantics struct {
	Durability  Durability `json:"durability"`
	Acks        int        `json:"acks"`
	Idempotence bool       `json:"idempotence"`
	Isolation   string     `json:"isolation"`
	Security    Security   `json:"security"`
}

type Durability struct {
	ReplicationFactor int `json:"replication_factor"`
	MinInsyncReplicas int `json:"min_insync_replicas"`
}

type Security struct {
	Protocol      string `json:"protocol"`
	SASLMechanism string `json:"sasl_mechanism,omitempty"`
}

type Provenance struct {
	Source     Source     `json:"source"`
	Binary     Binary     `json:"binary"`
	Config     ConfigProv `json:"config"`
	Toolchains Toolchains `json:"toolchains"`
	Broker     BrokerProv `json:"broker"`
	Host       Host       `json:"host"`
	Topology   Topology   `json:"topology"`
	Timestamps Timestamps `json:"timestamps"`
	Seeds      Seeds      `json:"seeds"`
	Artifacts  []Artifact `json:"artifacts"`
}

type Source struct {
	GitCommit string `json:"git_commit"`
	GitBranch string `json:"git_branch"`
	RepoURL   string `json:"repo_url"`
	Clean     bool   `json:"clean"`
	TreeHash  string `json:"tree_hash"`
	Note      string `json:"note,omitempty"`
}

type Binary struct {
	Name   string `json:"name"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type ConfigProv struct {
	Path              string         `json:"path"`
	SHA256            string         `json:"sha256"`
	EffectiveSettings map[string]any `json:"effective_settings"`
}

type Toolchains struct {
	Compiler  string `json:"compiler"`
	Runtime   string `json:"runtime"`
	BuildTool string `json:"build_tool"`
	FranzGo   string `json:"franzgo"`
	Kadm      string `json:"kadm"`
}

type BrokerProv struct {
	Image     string   `json:"image"`
	Version   string   `json:"version"`
	Mode      string   `json:"mode"`
	ClusterID string   `json:"cluster_id"`
	NodeCount int      `json:"node_count"`
	Endpoints []string `json:"endpoints"`
}

type Host struct {
	Hostname      string  `json:"hostname"`
	OS            string  `json:"os"`
	OSFamily      string  `json:"os_family"`
	KernelVersion string  `json:"kernel_version"`
	Arch          string  `json:"arch"`
	CPU           HostCPU `json:"cpu"`
	Memory        HostMem `json:"memory"`
}

type HostCPU struct {
	Model         string  `json:"model"`
	PhysicalCores int     `json:"physical_cores"`
	LogicalCores  int     `json:"logical_cores"`
	FrequencyMHz  float64 `json:"frequency_mhz"`
}

type HostMem struct {
	TotalBytes int64  `json:"total_bytes"`
	Unit       string `json:"unit"`
}

type Topology struct {
	Environment      string  `json:"environment"`
	RTTMs            float64 `json:"rtt_ms"`
	RTTUnit          string  `json:"rtt_unit"`
	ClientNodes      int     `json:"client_nodes"`
	BrokerNodes      int     `json:"broker_nodes"`
	NetworkInterface string  `json:"network_interface"`
}

type Timestamps struct {
	StartUTC        string  `json:"start_time_utc"`
	EndUTC          string  `json:"end_time_utc"`
	DurationSeconds float64 `json:"duration_seconds"`
	DurationUnit    string  `json:"duration_unit"`
}

type Seeds struct {
	PayloadSeed    string `json:"payload_seed"`
	KeySeed        string `json:"key_seed"`
	PartitionSeed  string `json:"partition_seed"`
	RepetitionSeed string `json:"repetition_seed"`
}

type Artifact struct {
	Path      string `json:"path"`
	Type      string `json:"type"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

type Execution struct {
	Phase                    string      `json:"phase"`
	WarmupCompleted          bool        `json:"warmup_completed"`
	WarmupRecords            uint64      `json:"warmup_records"`
	WarmupDurationSeconds    float64     `json:"warmup_duration_seconds"`
	SteadyDurationSeconds    float64     `json:"steady_state_duration_seconds"`
	RepetitionIndex          int         `json:"repetition_index"`
	TotalRepetitions         int         `json:"total_repetitions"`
	PairingOrder             string      `json:"pairing_order"`
	CoordinatedOmissionAvoid Coordinated `json:"coordinated_omission_avoidance"`
}

type Coordinated struct {
	Enabled      bool   `json:"enabled"`
	ScheduleType string `json:"schedule_type"`
}

type Outcomes struct {
	Offered      int64 `json:"offered"`
	Accepted     int64 `json:"accepted"`
	Acknowledged int64 `json:"acknowledged"`
	Consumed     int64 `json:"consumed"`
	Rejected     int64 `json:"rejected"`
	TimedOut     int64 `json:"timed_out"`
	Unknown      int64 `json:"unknown"`
}

type Measurements struct {
	Throughput      Throughput      `json:"throughput"`
	Latency         Latency         `json:"latency"`
	ClientResources ClientResources `json:"client_resources"`
	BrokerResources BrokerResources `json:"broker_resources"`
	Errors          []ErrorEvent    `json:"errors"`
}

type Throughput struct {
	RecordsPerSecond float64 `json:"records_per_second"`
	RecordsUnit      string  `json:"records_per_second_unit"`
	MBPerSecond      float64 `json:"megabytes_per_second"`
	MBUnit           string  `json:"megabytes_per_second_unit"`
	TotalBytes       int64   `json:"total_bytes_transferred"`
	TotalBytesUnit   string  `json:"total_bytes_unit"`
	MBDefinition     string  `json:"mb_definition"`
}

type Latency struct {
	SampleCount int       `json:"sample_count"`
	Unit        string    `json:"unit"`
	P50         float64   `json:"p50"`
	P90         float64   `json:"p90"`
	P95         float64   `json:"p95"`
	P99         float64   `json:"p99"`
	P999        float64   `json:"p99_9"`
	Min         float64   `json:"min"`
	Max         float64   `json:"max"`
	Mean        float64   `json:"mean"`
	Stddev      float64   `json:"stddev"`
	CI95        CI        `json:"confidence_interval_95"`
	Histogram   Histogram `json:"raw_histogram"`
	Definition  string    `json:"definition"`
}

type CI struct {
	Lower  float64 `json:"lower"`
	Upper  float64 `json:"upper"`
	Unit   string  `json:"unit"`
	Method string  `json:"method"`
}

type Histogram struct {
	BucketUnit string   `json:"bucket_unit"`
	Buckets    []Bucket `json:"buckets"`
}

type Bucket struct {
	MinUs float64 `json:"min_us"`
	MaxUs float64 `json:"max_us"`
	Count int64   `json:"count"`
}

type ClientResources struct {
	CPUUtilizationPct float64     `json:"cpu_utilization_pct"`
	CPUUnit           string      `json:"cpu_unit"`
	UserCPUSeconds    float64     `json:"user_cpu_seconds"`
	SystemCPUSeconds  float64     `json:"system_cpu_seconds"`
	CPUSecondsUnit    string      `json:"cpu_seconds_unit"`
	Allocations       Allocations `json:"allocations"`
	RSS               RSS         `json:"rss"`
	ThreadsCount      int         `json:"threads_count"`
	ThreadsNote       string      `json:"threads_note"`
}

type Allocations struct {
	TotalAllocatedBytes int64  `json:"total_allocated_bytes"`
	AllocationCount     int64  `json:"allocation_count"`
	Unit                string `json:"unit"`
}

type RSS struct {
	PeakRSSBytes    int64  `json:"peak_rss_bytes"`
	AverageRSSBytes int64  `json:"average_rss_bytes"`
	Unit            string `json:"unit"`
}

type BrokerResources struct {
	CPUUtilizationPct float64 `json:"cpu_utilization_pct"`
	CPUUnit           string  `json:"cpu_unit"`
	PeakRSSBytes      int64   `json:"peak_rss_bytes"`
	RSSUnit           string  `json:"rss_unit"`
	DiskWriteBytes    int64   `json:"disk_write_bytes"`
	DiskWriteUnit     string  `json:"disk_write_unit"`
	Note              string  `json:"note"`
}

type ErrorEvent struct {
	Code  string `json:"code"`
	Name  string `json:"name"`
	Count int64  `json:"count"`
	Fatal bool   `json:"fatal"`
	Phase string `json:"phase"`
}

type Integrity struct {
	Verified                    bool          `json:"verified"`
	RecordIDs                   RecordIDs     `json:"record_ids"`
	HighWatermarkAudit          HighWatermark `json:"high_watermark_audit"`
	IdempotenceSequenceVerified bool          `json:"idempotence_sequence_verified"`
	IntegrityFailure            bool          `json:"integrity_failure"`
}

type RecordIDs struct {
	StartID              int64  `json:"start_id"`
	EndID                int64  `json:"end_id"`
	ExpectedCount        int64  `json:"expected_count"`
	VerifiedCount        int64  `json:"verified_count"`
	MissingIDsCount      int64  `json:"missing_ids_count"`
	DuplicateIDsCount    int64  `json:"duplicate_ids_count"`
	ChecksumAlgorithm    string `json:"checksum_algorithm"`
	PayloadChecksumMatch bool   `json:"payload_checksum_matches"`
}

type HighWatermark struct {
	Partitions          []PartitionHW `json:"partitions"`
	TotalOffsetDelta    int64         `json:"total_offset_delta"`
	MatchesAcknowledged bool          `json:"matches_acknowledged"`
}

type PartitionHW struct {
	Partition   int   `json:"partition"`
	StartOffset int64 `json:"start_offset"`
	EndOffset   int64 `json:"end_offset"`
	OffsetDelta int64 `json:"offset_delta"`
}

type RepetitionHist struct {
	TotalAttempts  int       `json:"total_attempts"`
	FailedAttempts int       `json:"failed_attempts"`
	Attempts       []Attempt `json:"attempts"`
}

type Attempt struct {
	AttemptNumber    int     `json:"attempt_number"`
	RepetitionIndex  int     `json:"repetition_index"`
	Status           string  `json:"status"`
	IntegrityFailure bool    `json:"integrity_failure"`
	ErrorMessage     *string `json:"error_message"`
	TimestampUTC     string  `json:"timestamp_utc"`
}

// LatencyStats computes percentiles, moments, a fixed-seed bootstrap
// 95% CI of the mean, and log-spaced histogram buckets over
// microsecond samples.
func LatencyStats(us []float64) Latency {
	out := Latency{Unit: "microseconds", Definition: "closed-loop per-record produce-ack latency (promise resolve minus produce call)"}
	out.SampleCount = len(us)
	if len(us) == 0 {
		out.Histogram = Histogram{BucketUnit: "microseconds", Buckets: []Bucket{{MinUs: 0, MaxUs: 0, Count: 0}}}
		out.CI95 = CI{Unit: "microseconds", Method: "bootstrap-mean-1000-seed-1"}
		return out
	}
	sorted := append([]float64(nil), us...)
	sort.Float64s(sorted)
	quantile := func(q float64) float64 {
		if len(sorted) == 1 {
			return sorted[0]
		}
		pos := q * float64(len(sorted)-1)
		lo := int(math.Floor(pos))
		hi := int(math.Ceil(pos))
		if lo == hi {
			return sorted[lo]
		}
		return sorted[lo] + (sorted[hi]-sorted[lo])*(pos-float64(lo))
	}
	out.P50 = quantile(0.50)
	out.P90 = quantile(0.90)
	out.P95 = quantile(0.95)
	out.P99 = quantile(0.99)
	out.P999 = quantile(0.999)
	out.Min = sorted[0]
	out.Max = sorted[len(sorted)-1]
	var sum float64
	for _, v := range us {
		sum += v
	}
	out.Mean = sum / float64(len(us))
	var sq float64
	for _, v := range us {
		d := v - out.Mean
		sq += d * d
	}
	out.Stddev = math.Sqrt(sq / float64(len(us)))
	lo, hi := bootstrapMeanCI(us, out.Mean)
	out.CI95 = CI{Lower: lo, Upper: hi, Unit: "microseconds", Method: "bootstrap-mean-1000-seed-1"}
	out.Histogram = Histogram{BucketUnit: "microseconds", Buckets: logBuckets(sorted)}
	return out
}

// bootstrapMeanCI resamples the mean 1000x under a fixed splitmix64
// stream and returns the 2.5/97.5 percentiles.
func bootstrapMeanCI(us []float64, _ float64) (float64, float64) {
	const resamples = 1000
	n := len(us)
	means := make([]float64, resamples)
	state := uint64(1)
	next := func() uint64 {
		state += 0x9E3779B97F4A7C15
		z := state
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		return z ^ (z >> 31)
	}
	for r := 0; r < resamples; r++ {
		var sum float64
		for i := 0; i < n; i++ {
			sum += us[next()%uint64(n)]
		}
		means[r] = sum / float64(n)
	}
	sort.Float64s(means)
	return means[int(0.025*resamples)], means[int(0.975*resamples)]
}

// logBuckets covers [min, max] with ~24 log-spaced buckets.
func logBuckets(sorted []float64) []Bucket {
	lo := sorted[0]
	hi := sorted[len(sorted)-1]
	if hi <= lo {
		return []Bucket{{MinUs: lo, MaxUs: hi, Count: int64(len(sorted))}}
	}
	const nb = 24
	logLo := math.Log(math.Max(lo, 1e-9))
	logHi := math.Log(hi)
	edges := make([]float64, nb+1)
	for i := 0; i <= nb; i++ {
		edges[i] = math.Exp(logLo + (logHi-logLo)*float64(i)/nb)
	}
	edges[0] = math.Min(edges[0], lo)
	edges[nb] = math.Max(edges[nb], hi)
	counts := make([]int64, nb)
	for _, v := range sorted {
		idx := sort.Search(nb, func(i int) bool { return v < edges[i+1] })
		if idx >= nb {
			idx = nb - 1
		}
		counts[idx]++
	}
	var out []Bucket
	for i := 0; i < nb; i++ {
		if counts[i] > 0 {
			out = append(out, Bucket{MinUs: edges[i], MaxUs: edges[i+1], Count: counts[i]})
		}
	}
	return out
}

// SelfSHA256 hashes the running binary for provenance.binary.
func SelfSHA256() (path string, sum string, err error) {
	path, err = os.Executable()
	if err != nil {
		return "", "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	digest := sha256.Sum256(data)
	return path, hex.EncodeToString(digest[:]), nil
}

// GitProvenance shells out to git once; failures record "unknown"
// rather than failing the run (the result file stays fail-closed via
// its other required fields, and Clean=false marks doubt).
func GitProvenance() (commit, branch string, clean bool, tree string) {
	commit, branch, tree = "unknown", "unknown", "unknown"
	if out, err := exec.Command("git", "rev-parse", "HEAD").Output(); err == nil {
		commit = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("git", "rev-parse", "--abbrev-ref", "HEAD").Output(); err == nil {
		branch = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("git", "status", "--porcelain").Output(); err == nil {
		clean = len(bytes_TrimSpace(out)) == 0
	}
	if out, err := exec.Command("git", "rev-parse", "HEAD^{tree}").Output(); err == nil {
		tree = strings.TrimSpace(string(out))
	}
	return commit, branch, clean, tree
}

func bytes_TrimSpace(b []byte) []byte {
	return []byte(strings.TrimSpace(string(b)))
}

// BuildVersions reports the franz-go and kadm module versions from the
// binary's build info (the go.mod pins, resolved at link time).
func BuildVersions() (franzgo, kadm string) {
	franzgo, kadm = "unknown", "unknown"
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return franzgo, kadm
	}
	for _, m := range info.Deps {
		switch m.Path {
		case "github.com/twmb/franz-go":
			franzgo = m.Version
		case "github.com/twmb/franz-go/pkg/kadm":
			kadm = m.Version
		}
	}
	return franzgo, kadm
}

// HostProbe collects schema-required host facts with documented
// fallbacks when a source is unavailable.
func HostProbe() Host {
	hostname, _ := os.Hostname()
	goarch := runtime.GOARCH
	arch := goarch
	if goarch == "amd64" {
		arch = "x86_64"
	}
	h := Host{
		Hostname:      firstNonEmpty(hostname, "unknown"),
		OS:            runtime.GOOS + "/" + goarch,
		OSFamily:      runtime.GOOS,
		KernelVersion: kernelVersion(),
		Arch:          arch,
		CPU: HostCPU{
			Model:         cpuModel(),
			PhysicalCores: physicalCores(),
			LogicalCores:  runtime.NumCPU(),
			FrequencyMHz:  cpuMHz(),
		},
		Memory: HostMem{TotalBytes: totalMemory(), Unit: "bytes"},
	}
	if h.CPU.PhysicalCores < 1 {
		h.CPU.PhysicalCores = h.CPU.LogicalCores
	}
	if h.Memory.TotalBytes < 1 {
		h.Memory.TotalBytes = 1
	}
	return h
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

func sysctlString(name string) string {
	out, err := exec.Command("sysctl", "-n", name).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func kernelVersion() string {
	var uts unix.Utsname
	if err := unix.Uname(&uts); err != nil {
		return "unknown"
	}
	chars := uts.Release[:]
	n := 0
	for n < len(chars) && chars[n] != 0 {
		n++
	}
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		out[i] = byte(chars[i])
	}
	return string(out)
}

func cpuModel() string {
	if runtime.GOOS == "darwin" {
		if m := sysctlString("machdep.cpu.brand_string"); m != "" {
			return m
		}
	}
	if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "model name") {
				if _, v, ok := strings.Cut(line, ":"); ok {
					return strings.TrimSpace(v)
				}
			}
		}
	}
	return "unknown"
}

func physicalCores() int {
	if runtime.GOOS == "darwin" {
		if v, err := strconv.Atoi(sysctlString("hw.physicalcpu")); err == nil {
			return v
		}
	}
	if data, err := os.ReadFile("/sys/devices/system/cpu/possible"); err == nil {
		// "0-7" style ranges.
		total := 0
		for _, part := range strings.Split(strings.TrimSpace(string(data)), ",") {
			if lo, hi, ok := strings.Cut(part, "-"); ok {
				a, _ := strconv.Atoi(strings.TrimSpace(lo))
				b, _ := strconv.Atoi(strings.TrimSpace(hi))
				total += b - a + 1
			} else if _, err := strconv.Atoi(strings.TrimSpace(part)); err == nil {
				total++
			}
		}
		if total > 0 {
			return total
		}
	}
	return 0
}

func cpuMHz() float64 {
	if runtime.GOOS == "darwin" {
		if v, err := strconv.ParseFloat(sysctlString("hw.cpufrequency"), 64); err == nil {
			return v / 1e6
		}
		return 0
	}
	if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "cpu MHz") {
				if _, v, ok := strings.Cut(line, ":"); ok {
					if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
						return f
					}
				}
			}
		}
	}
	return 0
}

func totalMemory() int64 {
	if runtime.GOOS == "darwin" {
		if v, err := strconv.ParseInt(sysctlString("hw.memsize"), 10, 64); err == nil {
			return v
		}
	}
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
						return kb * 1024
					}
				}
			}
		}
	}
	return 0
}

// ResourceSnapshot captures process rusage + Go memstats for
// measurements.client_resources.
type ResourceSnapshot struct {
	UserSeconds   float64
	SystemSeconds float64
	PeakRSSBytes  int64
	TotalAlloc    uint64
	Mallocs       uint64
}

// SnapshotResources reads rusage(RUSAGE_SELF) and Go memstats.
func SnapshotResources() ResourceSnapshot {
	var snap ResourceSnapshot
	var ru unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &ru); err == nil {
		snap.UserSeconds = float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
		snap.SystemSeconds = float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
		// ru_maxrss is bytes on macOS, kilobytes on Linux.
		snap.PeakRSSBytes = int64(ru.Maxrss)
		if runtime.GOOS == "linux" {
			snap.PeakRSSBytes *= 1024
		}
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	snap.TotalAlloc = mem.TotalAlloc
	snap.Mallocs = mem.Mallocs
	return snap
}

// FormatTime renders UTC RFC3339 for schema timestamps.
func FormatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}
