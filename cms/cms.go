package cms

import (
	"encoding/binary"

	"fmt"
	"math"
	"os"
)

// CMS predstavlja Count-Min Sketch strukturu
type CMS struct {
	Matrix    [][]uint32     // k redova, m kolona
	Depth     uint           // broj hash funkcija (k)
	Width     uint           // broj kolona (m)
	HashFuncs []HashWithSeed // hash funkcija sa 32-bajt seed-ovima
}

// NewCMS kreira novi CMS na osnovu zeljene greske i verovatnoce
func NewCMS(epsilon, delta float64) *CMS {
	width := CalculateM(epsilon)
	depth := CalculateK(delta)

	matrix := make([][]uint32, depth)
	for i := range matrix {
		matrix[i] = make([]uint32, width)
	}

	hashFuncs := CreateHashFunctions(depth)

	return &CMS{
		Matrix:    matrix,
		Depth:     depth,
		Width:     width,
		HashFuncs: hashFuncs,
	}
}

// Add povecava brojanje za dati kljuc
func (cms *CMS) Add(key string) {
	data := []byte(key)

	for i := uint(0); i < cms.Depth; i++ {
		index := cms.HashFuncs[i].Hash(data) % uint64(cms.Width)
		cms.Matrix[i][index]++
	}
}

// Estimate vraca procenjeno pojavljivanje kljuca
func (cms *CMS) Estimate(key string) uint32 {
	data := []byte(key)
	min := uint32(math.MaxUint32)
	for i := uint(0); i < cms.Depth; i++ {
		index := cms.HashFuncs[i].Hash(data) % uint64(cms.Width)
		val := cms.Matrix[i][index]
		if val < min {
			min = val
		}
	}
	return min
}

// SaveToFile binarno snima CMS u fajl
func (cms *CMS) SaveToFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Snimi dimenzije
	if err := binary.Write(file, binary.LittleEndian, uint32(cms.Depth)); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, uint32(cms.Width)); err != nil {
		return err
	}

	// Snimi sve hash seedove (32 bajta svaki)
	for _, hf := range cms.HashFuncs {
		if len(hf.Seed) != 32 {
			return fmt.Errorf("seed nije 32 bajta")
		}
		if _, err := file.Write(hf.Seed); err != nil {
			return err
		}
	}

	// Snimi celu matricu
	for i := uint(0); i < cms.Depth; i++ {
		for j := uint(0); j < cms.Width; j++ {
			if err := binary.Write(file, binary.LittleEndian, cms.Matrix[i][j]); err != nil {
				return err
			}
		}
	}

	return nil
}

// LoadFromFile učitava CMS iz fajla
func LoadFromFile(path string) (*CMS, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var depth32, width32 uint32
	if err := binary.Read(file, binary.LittleEndian, &depth32); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &width32); err != nil {
		return nil, err
	}

	depth := uint(depth32)
	width := uint(width32)

	// Učitaj hash funkcije (32 bajta po seedu)
	hashFuncs := make([]HashWithSeed, depth)
	for i := uint(0); i < depth; i++ {
		seed := make([]byte, 32)
		if _, err := file.Read(seed); err != nil {
			return nil, err
		}
		hashFuncs[i] = HashWithSeed{Seed: seed}
	}

	// Učitaj matricu
	matrix := make([][]uint32, depth)
	for i := range matrix {
		matrix[i] = make([]uint32, width)
		for j := uint(0); j < width; j++ {
			if err := binary.Read(file, binary.LittleEndian, &matrix[i][j]); err != nil {
				return nil, err
			}
		}
	}

	return &CMS{
		Matrix:    matrix,
		Depth:     depth,
		Width:     width,
		HashFuncs: hashFuncs,
	}, nil
}
