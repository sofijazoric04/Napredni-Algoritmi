<<<<<<<< HEAD:cms/hash.go
package cms
========
package bloomfilter
>>>>>>>> eb381aa02d7357fc6a96f9d95944d04442f02395:bloomfilter/hash.go

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

func CreateHashFunctions(k uint) []HashWithSeed {
	h := make([]HashWithSeed, k)
<<<<<<<< HEAD:cms/hash.go
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		seed := make([]byte, 32)
		binary.BigEndian.PutUint32(seed, uint32(ts+i))
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
========
	for i := uint32(0); i < k; i++ {
		seed := make([]byte, 4)
		binary.BigEndian.PutUint32(seed, i)
		h[i] = HashWithSeed{Seed: seed}
>>>>>>>> eb381aa02d7357fc6a96f9d95944d04442f02395:bloomfilter/hash.go
	}
	return h
}
