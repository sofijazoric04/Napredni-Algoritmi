package bloomfilter

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
