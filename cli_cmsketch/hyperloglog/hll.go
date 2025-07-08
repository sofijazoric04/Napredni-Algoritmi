package hyperloglog

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"math"
	"math/bits"
	"os"
)

// HLL je struktura HyperLogLog
// Preciznost p odredjuje broj registara: m = 2^p
// Reg je niz registara duzine m, svaki pamti max broj nula
type HLL struct {
	P   uint8   // preciznost
	M   uint32  // broj registara (2^p)
	Reg []uint8 // niz registara
}

// NewHLL pravi novi prazan HLL sa zadatom precinznoscu p (najcesce 4 <= p <= 16)
func NewHLL(p uint8) *HLL {
	if p < 4 || p > 16 {
		panic("Preciznost p mora biti u opsegu [4,16]")
	}
	m := uint32(1) << p
	return &HLL{
		P:   p,
		M:   m,
		Reg: make([]uint8, m),
	}
}

// Add dodaje novi string u HLL
func (h *HLL) Add(data string) {
	hasher := fnv.New64a()
	hasher.Write([]byte(data))
	hash := hasher.Sum64()

	// index = prvih p bita
	index := hash >> (64 - h.P)

	// ostatak = 64 - p bitova, broj nula + 1
	remaining := (hash << h.P) | (1 << (h.P - 1))
	zeros := trailingZeroBits(remaining) + 1

	if zeros > h.Reg[index] {
		h.Reg[index] = zeros
	}
}

// Estimate vraca procenu broja jedinstvenih elemenata
func (h *HLL) Estimate() float64 {
	alpha := 0.7213 / (1 + 1.079/float64(h.M))
	Z := 0.0
	for _, reg := range h.Reg {
		Z += 1.0 / math.Pow(2.0, float64(reg))
	}

	estimate := alpha * float64(h.M*h.M) / Z

	// Korekcija za male brojeve
	if estimate <= 5.0/2.0*float64(h.M) {
		V := float64(emptyCount(h.Reg))
		if V > 0 {
			return float64(h.M) * math.Log(float64(h.M)/V)
		}
	}

	return estimate
}


// SaveToFile snima HLL u binarni fajl
func (h *HLL) SaveToFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := binary.Write(file, binary.LittleEndian, h.P); err != nil {
		return err
	}
	for _, val := range h.Reg {
		if err := binary.Write(file, binary.LittleEndian, val); err != nil {
			return err
		}
	}
	return nil
}

// LoadFromFile ucitava HLL iz fajla
func LoadFromFile(path string) (*HLL, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var p uint8
	if err := binary.Read(file, binary.LittleEndian, &p); err != nil {
		return nil, err
	}
	if p < 4 || p > 16 {
		return nil, errors.New("neispravna preciznost u fajlu")
	}

	m := uint32(1) << p
	reg := make([]uint8, m)
	for i := range reg {
		if err := binary.Read(file, binary.LittleEndian, &reg[i]); err != nil {
			return nil, err
		}
	}

	return &HLL{
		P:   p,
		M:   m,
		Reg: reg,
	}, nil
}


// trailingZeroBits broji broj uzastopnih nula s desna
func trailingZeroBits(x uint64) uint8 {
	return uint8(bits.TrailingZeros64(x))
}

// emptyCount vraca broj registara koji su 0
func emptyCount(reg []uint8) int {
	count := 0
	for _, val := range reg {
		if val == 0 {
			count++
		}
	}
	return count
}
