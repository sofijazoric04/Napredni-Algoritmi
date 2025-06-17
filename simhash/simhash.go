package simhash

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"math/bits"
	"os"
	"strings"
)

// SimHash predstavlja 64-bitni fingerprint teksta
// Predstavljen kao uint64 vrednost

type SimHash struct {
	Hash uint64
}

// NewSimHashFromText prima tekst i računa SimHash otisak
func NewSimHashFromText(text string) SimHash {
	words := strings.Fields(text) // razdvajanje po rečima
	vector := make([]int, 64)     // vektor težina po bitovima (pozitivno/negativno)

	for _, word := range words {
		hash := md5.Sum([]byte(word))
		val := binary.BigEndian.Uint64(hash[:8]) // koristi prvih 8 bajtova (64 bita)

		for i := 0; i < 64; i++ {
			if (val>>i)&1 == 1 {
				vector[i]++ // bit je 1 → dodaj +1
			} else {
				vector[i]-- // bit je 0 → oduzmi -1
			}
		}
	}

	// Konačni hash: ako je suma > 0 → 1, inače 0
	var final uint64 = 0
	for i := 0; i < 64; i++ {
		if vector[i] > 0 {
			final |= (1 << i)
		}
	}

	return SimHash{Hash: final}
}

// HammingDistance računa broj različitih bitova između dva otiska
func HammingDistance(a, b SimHash) int {
	return bits.OnesCount64(a.Hash ^ b.Hash)
}

// SaveToFile čuva fingerprint kao binarni fajl
func (s SimHash) SaveToFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return binary.Write(file, binary.LittleEndian, s.Hash)
}

// LoadFromFile učitava fingerprint iz fajla
func LoadFromFile(path string) (SimHash, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return SimHash{}, err
	}
	if len(data) != 8 {
		return SimHash{}, errors.New("neispravan format fajla")
	}
	hash := binary.LittleEndian.Uint64(data)
	return SimHash{Hash: hash}, nil
}
