package main

import (
	"bloom_filter/bfhelpers"
	"fmt"
)

type BloomFilter struct {
	p             float64
	m             uint
	k             uint
	hashFunctions []bfhelpers.HashWithSeed
	bitset        []byte
}

// Konstruktor za BloomFilter
func NewBloomFilter(p float64) *BloomFilter {
	m := bfhelpers.CalculateM(100, p)
	k := bfhelpers.CalculateK(100, m)

	hashFunctions := bfhelpers.CreateHashFunctions(uint32(k))
	bitset := make([]byte, m/8+1) // ako nije deljivo sa 8, onda +1

	return &BloomFilter{
		p:             p,
		m:             m,
		k:             k,
		hashFunctions: hashFunctions,
		bitset:        bitset,
	}
}

func DeleteBloomFilter(bf *BloomFilter) {
	bf.bitset = nil
	bf.hashFunctions = nil
}

func (bf *BloomFilter) Add(data []byte) {
	for _, hashFunc := range bf.hashFunctions {
		hash := hashFunc.Hash(data)
		index := hash % uint64(bf.m)
		bf.bitset[index/8] |= (1 << (index % 8)) // Postavljanje bita na 1, nadje se indeks bajta i indeks bita u bajtu
	}
}

func (bf *BloomFilter) Contains(data []byte) bool {
	for _, hashFunc := range bf.hashFunctions {
		hash := hashFunc.Hash(data)
		index := hash % uint64(bf.m)
		if bf.bitset[index/8]&(1<<(index%8)) == 0 {
			// podatak sigurno nije u filteru
			return false
		}
	}
	return true // svi 1 => podatak je verovatno u filteru
}

func main() {
	// Ovo je samo za testiranje, necete vi koristiti ovaj main
	bf := NewBloomFilter(0.001)
	fmt.Printf("Bloom filter kreiran sa p: %f i k: %d\n", bf.p, bf.k)

	// Dodavanje podataka
	data := []byte("primer")
	bf.Add(data)

	// Provera da li se podatak nalazi u njemu
	fmt.Printf("%s se nalazi u Bloom filteru: %t\n", data, bf.Contains(data))

	// Provera podatka koji nije u njemu
	data2 := []byte("neki levi")
	fmt.Printf("%s se nalazi u Bloom filteru: %t\n", data2, bf.Contains(data2))
}
