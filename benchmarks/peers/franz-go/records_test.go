package peer

import (
	"bytes"
	"testing"
)

// Test vectors pin the deterministic record scheme: any consumer in
// any language regenerates these exact bytes.
func TestRecordSchemeVectors(t *testing.T) {
	key0 := MakeKey(0, DefaultSeed)
	if len(key0) != KeyLen {
		t.Fatalf("key len = %d, want %d", len(key0), KeyLen)
	}
	// Index 0 embeds be64(0) + splitmix64(seed).
	var wantDist [8]byte
	s := splitmix64(DefaultSeed)
	wantDist[0] = byte(s >> 56)
	wantDist[1] = byte(s >> 48)
	wantDist[2] = byte(s >> 40)
	wantDist[3] = byte(s >> 32)
	wantDist[4] = byte(s >> 24)
	wantDist[5] = byte(s >> 16)
	wantDist[6] = byte(s >> 8)
	wantDist[7] = byte(s)
	if !bytes.Equal(key0[8:16], wantDist[:]) {
		t.Fatalf("key distinguisher = %x, want %x", key0[8:16], wantDist)
	}
	for i := range key0[:8] {
		if key0[i] != 0 {
			t.Fatalf("key index prefix = %x, want zeros", key0[:8])
		}
	}
	// Values differ per index but are stable across calls.
	v1a := MakeValue(1, DefaultSeed, 100)
	v1b := MakeValue(1, DefaultSeed, 100)
	v2 := MakeValue(2, DefaultSeed, 100)
	if !bytes.Equal(v1a, v1b) {
		t.Fatal("MakeValue not deterministic")
	}
	if bytes.Equal(v1a, v2) {
		t.Fatal("MakeValue collides across indices")
	}
	if len(v1a) != 100 {
		t.Fatalf("value len = %d, want 100", len(v1a))
	}
}

func TestVerifyRoundtrip(t *testing.T) {
	for _, i := range []uint64{0, 1, 42, 1 << 32, ^uint64(0)} {
		key := MakeKey(i, DefaultSeed)
		val := MakeValue(i, DefaultSeed, 100)
		got, err := Verify(key, val, DefaultSeed, 100)
		if err != nil {
			t.Fatalf("Verify(%d): %v", i, err)
		}
		if got != i {
			t.Fatalf("Verify index = %d, want %d", got, i)
		}
	}
}

func TestVerifyRejects(t *testing.T) {
	key := MakeKey(7, DefaultSeed)
	val := MakeValue(7, DefaultSeed, 100)
	badKey := append([]byte(nil), key...)
	badKey[15] ^= 0xff
	if _, err := Verify(badKey, val, DefaultSeed, 100); err == nil {
		t.Fatal("Verify accepted a corrupted key")
	}
	badVal := append([]byte(nil), val...)
	badVal[50] ^= 0x01
	if _, err := Verify(key, badVal, DefaultSeed, 100); err == nil {
		t.Fatal("Verify accepted a corrupted payload")
	}
	if _, err := Verify(key, val[:99], DefaultSeed, 100); err == nil {
		t.Fatal("Verify accepted a short payload")
	}
	if _, err := Verify(key[:8], val, DefaultSeed, 100); err == nil {
		t.Fatal("Verify accepted a short key")
	}
	// Wrong seed fails: IDs are seed-scoped.
	if _, err := Verify(key, val, DefaultSeed+1, 100); err == nil {
		t.Fatal("Verify accepted a foreign seed")
	}
}

func TestLatencyStatsShape(t *testing.T) {
	us := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	lat := LatencyStats(us)
	if lat.SampleCount != 10 || lat.Unit != "microseconds" {
		t.Fatalf("shape: %+v", lat)
	}
	if lat.P50 != 55 || lat.Min != 10 || lat.Max != 100 || lat.Mean != 55 {
		t.Fatalf("moments: %+v", lat)
	}
	if !(lat.CI95.Lower <= lat.Mean && lat.Mean <= lat.CI95.Upper) {
		t.Fatalf("CI does not cover mean: %+v", lat.CI95)
	}
	if lat.Histogram.BucketUnit != "microseconds" || len(lat.Histogram.Buckets) == 0 {
		t.Fatalf("histogram: %+v", lat.Histogram)
	}
	var total int64
	for _, b := range lat.Histogram.Buckets {
		total += b.Count
	}
	if total != 10 {
		t.Fatalf("histogram counts sum to %d, want 10", total)
	}
	empty := LatencyStats(nil)
	if empty.SampleCount != 0 || len(empty.Histogram.Buckets) == 0 {
		t.Fatalf("empty stats: %+v", empty)
	}
}
