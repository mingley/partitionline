// Package peer is a pinned franz-go benchmark peer for the
// equal-semantics contract (docs/benchmark-contract.md, KL09-65).
//
// Delivery definitions match partitionline: a record counts as
// acknowledged only when the broker's Produce response arrives
// (franz-go promise result with nil error). Enqueue (Produce call
// acceptance) is never counted as an acknowledgment.
package peer

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

// DefaultSeed matches the contract's uniform-random payload seed
// (matched-bulk-acks-all-6p-uncompressed frozen knobs).
const DefaultSeed uint64 = 0x5EED0001

// KeyLen is the deterministic 16-byte record key: the record index
// plus a seed-derived distinguisher.
const KeyLen = 16

// splitmix64 step, matching the reference integer stream used by the
// partitionline bench tooling for cross-client verification.
func splitmix64(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	z := x
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// MakeKey returns the deterministic 16-byte key for record index:
// big-endian index followed by big-endian splitmix64(seed^index).
// The embedded index lets any consumer verify without ordering
// assumptions.
func MakeKey(index uint64, seed uint64) []byte {
	key := make([]byte, KeyLen)
	binary.BigEndian.PutUint64(key[0:8], index)
	binary.BigEndian.PutUint64(key[8:16], splitmix64(seed^index))
	return key
}

// ExtractIndex recovers the record index from a key made by MakeKey.
func ExtractIndex(key []byte) (uint64, error) {
	if len(key) != KeyLen {
		return 0, errors.New("peer: key must be 16 bytes")
	}
	return binary.BigEndian.Uint64(key[0:8]), nil
}

// MakeValue returns n deterministic payload bytes for record index:
// the splitmix64 stream chained from (seed ^ index*GOLDEN), so every
// record is reproducible without consuming its predecessors.
func MakeValue(index uint64, seed uint64, n int) []byte {
	out := make([]byte, n)
	state := seed ^ (index * 0x9E3779B97F4A7C15)
	var buf [8]byte
	for off := 0; off < n; {
		state = splitmix64(state)
		binary.BigEndian.PutUint64(buf[:], state)
		end := off + 8
		if end > n {
			end = n
		}
		copy(out[off:end], buf[:end-off])
		off = end
	}
	return out
}

// Digest returns the sha256 of a payload, the checksum recorded in
// result documents (integrity.record_ids.checksum_algorithm).
func Digest(value []byte) [32]byte {
	return sha256.Sum256(value)
}

// Verify checks that key/value are exactly the deterministic bytes for
// some record index under seed, and returns that index. Payload size
// must equal the configured payload bytes.
func Verify(key, value []byte, seed uint64, payloadBytes int) (uint64, error) {
	index, err := ExtractIndex(key)
	if err != nil {
		return 0, err
	}
	wantKey := MakeKey(index, seed)
	for i := range wantKey {
		if key[i] != wantKey[i] {
			return 0, errors.New("peer: key distinguisher mismatch")
		}
	}
	if len(value) != payloadBytes {
		return 0, errors.New("peer: payload size mismatch")
	}
	wantValue := MakeValue(index, seed, payloadBytes)
	if Digest(value) != Digest(wantValue) {
		return 0, errors.New("peer: payload checksum mismatch")
	}
	return index, nil
}
