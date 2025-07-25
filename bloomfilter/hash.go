package bloomfilter

import (
	"crypto/md5"
	"encoding/binary"
)

type HashWithSeed struct {
	Seed []byte
}

func (h HashWithSeed) Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

func CreateHashFunctions(k uint32) []HashWithSeed {
	h := make([]HashWithSeed, k)
	for i := uint32(0); i < k; i++ {
		seed := make([]byte, 4)
		binary.BigEndian.PutUint32(seed, i) //  umesto ts+i
		h[i] = HashWithSeed{Seed: seed}
	}
	return h
}
