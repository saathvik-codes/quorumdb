package store

import (
	"crypto/sha1"
	"encoding/binary"
	"math"
)

// Bloom is a fixed-size Bloom filter using double hashing (Kirsch-Mitzenmacher)
// to derive k hash functions from two independent SHA-1-based hashes, avoiding
// the cost of k separate hash computations per key.
type Bloom struct {
	bits []uint64
	m    uint32 // number of bits
	k    uint32 // number of hash functions
}

// NewBloom sizes the filter for expectedItems keys at the given false-positive
// rate using the standard optimal-m/optimal-k formulas.
func NewBloom(expectedItems int, falsePositiveRate float64) *Bloom {
	if expectedItems < 1 {
		expectedItems = 1
	}
	m := optimalM(expectedItems, falsePositiveRate)
	k := optimalK(m, expectedItems)
	return &Bloom{
		bits: make([]uint64, (m+63)/64),
		m:    uint32(m),
		k:    uint32(k),
	}
}

func optimalM(n int, p float64) int {
	m := -1 * float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)
	if m < 8 {
		m = 8
	}
	return int(m)
}

func optimalK(m, n int) int {
	k := int(math.Round(float64(m) / float64(n) * math.Ln2))
	if k < 1 {
		k = 1
	}
	if k > 16 {
		k = 16
	}
	return k
}

func (b *Bloom) hashes(key string) (uint64, uint64) {
	sum := sha1.Sum([]byte(key))
	h1 := binary.BigEndian.Uint64(sum[0:8])
	h2 := binary.BigEndian.Uint64(sum[8:16])
	return h1, h2
}

func (b *Bloom) Add(key string) {
	h1, h2 := b.hashes(key)
	for i := uint32(0); i < b.k; i++ {
		bit := (h1 + uint64(i)*h2) % uint64(b.m)
		b.bits[bit/64] |= 1 << (bit % 64)
	}
}

// MightContain returns false only if key is definitely absent; true means
// "maybe present" (false positives are possible, false negatives are not).
func (b *Bloom) MightContain(key string) bool {
	h1, h2 := b.hashes(key)
	for i := uint32(0); i < b.k; i++ {
		bit := (h1 + uint64(i)*h2) % uint64(b.m)
		if b.bits[bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

func (b *Bloom) Serialize() []byte {
	out := make([]byte, 8+len(b.bits)*8)
	binary.BigEndian.PutUint32(out[0:4], b.m)
	binary.BigEndian.PutUint32(out[4:8], b.k)
	for i, w := range b.bits {
		binary.BigEndian.PutUint64(out[8+i*8:16+i*8], w)
	}
	return out
}

func DeserializeBloom(data []byte) *Bloom {
	if len(data) < 8 {
		return NewBloom(1, 0.01)
	}
	m := binary.BigEndian.Uint32(data[0:4])
	k := binary.BigEndian.Uint32(data[4:8])
	numWords := (len(data) - 8) / 8
	bits := make([]uint64, numWords)
	for i := 0; i < numWords; i++ {
		bits[i] = binary.BigEndian.Uint64(data[8+i*8 : 16+i*8])
	}
	return &Bloom{bits: bits, m: m, k: k}
}
