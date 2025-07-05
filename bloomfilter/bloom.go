package bloomfilter

import (
	"encoding/binary"
	//"fmt"
	"os"
)

type BloomFilter struct {
	Bitset    []bool         // niz bitova (bool radi jednostavnosti)
	Size      int            // velicina bitset-a (m)
	hashFuncs []HashWithSeed // lista hes funkcija, k hes funkcija
	NumHashes int            // broj hes funkcija (k)
}

// Funkcija kreira bloom filter, prima parametre:
// - expectedElements: ocekivani broj elemenata
// - falsePositiveRate: dozvoljena greska (procenat laznih pozitivnih)
func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := CalculateM(expectedElements, falsePositiveRate) // Izracunavamo optimalnu velicinu bitseta (m)
	k := CalculateK(expectedElements, m)                 // Izracunamo optimalan broj hes funkcija (k)
	hashFuncs := CreateHashFunctions(uint32(k))          // Kreiramo listu has funkcija

	return &BloomFilter{
		Bitset:    make([]bool, m), // svi bitovi su inicijalno "0"
		Size:      int(m),
		hashFuncs: hashFuncs,
		NumHashes: int(k),
	}
}

// Add dodaje kljuc u Bloom Filter postavljanjem k bitova na 1
func (bf *BloomFilter) Add(key string) {
	data := []byte(key) // konvertujemo kljuc u niz bajtova

	// Prolazimo kroz sve hes funkcije
	for _, hf := range bf.hashFuncs {
		// Izracunamo hes vrednost
		hashValue := hf.Hash(data)

		// Odredimo poziciju u bitsetu (moduo da ostanemo u granicama)
		pos := hashValue % uint64(bf.Size)

		// Postavimo odgovarajuci bit na 1
		bf.Bitset[pos] = true
	}
}

// MayContain proverava da li kljuc "mozda" postoji u filteru
func (bf *BloomFilter) MayContain(key string) bool {
	data := []byte(key)

	// Prolazimo kroz sve hes funkcije
	for _, hf := range bf.hashFuncs {
		// Izracunamo hes vrednost
		hashValue := hf.Hash(data)

		// Odredimo poziciju u bitsetu (moduo da ostanemo u granicama)
		pos := hashValue % uint64(bf.Size)

		// Ako je bilo koji bit na toj poziciji 0, kljuc sigurno ne postoji
		if !bf.Bitset[pos] {
			return false
		}
	}

	// Ako su svi bitovi 1, kljuc "mozda" postoji
	return true
}

// SaveToFile snima Bloom filter u binarni fajl koristeci GOB
func (bf *BloomFilter) SaveToFile(path string) error {

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// 1. Snimimo size (int64)
	if err := binary.Write(file, binary.LittleEndian, int64(bf.Size)); err != nil {
		return err
	}

	// 2. Snimamo numHashes - ovo je broj hes funkcija
	if err := binary.Write(file, binary.LittleEndian, int64(bf.NumHashes)); err != nil {
		return err
	}

	// 3. Snimamo svaki bit kao bajt (1 ili 0)
	for _, bit := range bf.Bitset {
		var b byte = 0
		if bit {
			b = 1
		}
		if err := binary.Write(file, binary.LittleEndian, b); err != nil {
			return err
		}
	}

	return nil

}

// LoadFromFile ucitava Bloom filter iz binarnog fajla
func LoadFromFile(path string) (*BloomFilter, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var size int64
	var numHashes int64

	// Ucitaj velicinu bitseta
	if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
		return nil, err
	}

	// Ucitaj broj has funkcija
	if err := binary.Read(file, binary.LittleEndian, &numHashes); err != nil {
		return nil, err
	}

	// Ucitaj sve bitove
	bitset := make([]bool, size)
	for i := int64(0); i < size; i++ {
		var b byte
		if err := binary.Read(file, binary.LittleEndian, &b); err != nil {
			return nil, err
		}
		bitset[i] = b == 1
	}

	// 4. Regeneriši hash funkcije
	hashFuncs := CreateHashFunctions(uint32(numHashes))

	// 5. Vrati gotov filter
	return &BloomFilter{
		Bitset:    bitset,
		Size:      int(size),
		NumHashes: int(numHashes),
		hashFuncs: hashFuncs,
	}, nil

}
