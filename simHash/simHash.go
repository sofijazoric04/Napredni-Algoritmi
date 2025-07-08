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

type SimHash struct {
	Hash uint64
}

func NewSimHashFromText(text string) SimHash {
	words := strings.Fields(text)
	vector := make([]int, 64)

	for _, word := range words {
		hash := md5.Sum([]byte(word))
		val := binary.BigEndian.Uint64(hash[:8])

		for i := 0; i < 64; i++ {
			if (val>>i)&1 == 1 {
				vector[i]++
			} else {
				vector[i]--
			}
		}
	}

	var final uint64 = 0
	for i := 0; i < 64; i++ {
		if vector[i] > 0 {
			final |= (1 << i)
		}
	}

	return SimHash{Hash: final}
}

func HammingDistance(a, b SimHash) int {
	return bits.OnesCount64(a.Hash ^ b.Hash)
}

func (s SimHash) SaveToFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return binary.Write(file, binary.LittleEndian, s.Hash)
}

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
