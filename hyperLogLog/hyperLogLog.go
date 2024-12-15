package main

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
)

// Maksimalna i minimalna preciznost
const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

// Funkcija iz helper file-a
func firstKbits(value, k uint64) uint64 {
	return value >> (64 - k)
}

// Funkcija iz helper file-a
func trailingZeroBits(value uint64) int {
	return bits.TrailingZeros64(value)
}

// HLL struct predstavlja HyperLogLog strukturu
type HLL struct {
	m   uint64  // Broj registara (2^p)
	p   uint8   // Preciznost (broj bitova za bucket)
	reg []uint8 // Initializacija svih registara na 0
}

// Inicijalizacija HLL strukture, vraca pokazivac na HLL strukturu i gresku ako je doslo do nje
func NewHLL(p uint8) (*HLL, error) {
	// Provera da li je preciznost u dozvoljenom opsegu
	if p < HLL_MIN_PRECISION || p > HLL_MAX_PRECISION {
		return nil, fmt.Errorf("precision p must be between %d and %d", HLL_MIN_PRECISION, HLL_MAX_PRECISION)
	}

	m := uint64(1) << p // Broj registara, pomeranje 1 za p mesta u levo, cime dobijamo 2 na p
	return &HLL{
		m:   m,
		p:   p,
		reg: make([]uint8, m), // Inicijalizacija svih registara na 0
	}, nil
}

func (hll *HLL) Add(value string) {
	// Hashiranje vrednosti
	hash := robustHash(value)

	// Odredjivanje indeksa bucket-a na osnovu prvih p bitova
	index := firstKbits(hash, uint64(hll.p))

	// Odredjivanje broja nula pomocu trailingZeroBits funkcije
	remainingBits := hash << hll.p
	zeros := trailingZeroBits(remainingBits) + 1 // +1 jer se broji i bucket

	// Ako je broj nula veci od trenutnog broja nula u registru, azuriraj vrednost
	if uint8(zeros) > hll.reg[index] {
		hll.reg[index] = uint8(zeros)
	}
}

func simpleHash(value string) uint64 {
	var hash uint64 = 0
	for i := 0; i < len(value); i++ {
		hash = hash*31 + uint64(value[i])
	}
	return hash

}

func robustHash(value string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(value))
	return h.Sum64()
}

// Funkcija iz helper file-a
func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

// Funkcija iz helper file-a
func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func main() {
	fmt.Println("Probabilistic structures")
	for {
		fmt.Println("1. Create probabilistic structure")
		fmt.Println("2. Add to probabilistic structure")
		fmt.Println("3. Query probabilistic structure")
		fmt.Println("4. Exit")
		fmt.Println("Choose option:")
		var choice int
		fmt.Scan(&choice)
		switch choice {
		case 1:
			createProbabilisticStructure()
		case 2:
			addToProbabilisticStructure()
		case 3:
			queryProbabilisticStructure()
		case 4:
			return
		default:
			fmt.Println("Invalid choice.")
		}
	}
}

var hyperLogLog *HLL

func createProbabilisticStructure() {
	fmt.Println("Enter precision for HyperLogLog (between 4 and 16):")
	var precision uint8
	fmt.Scan(&precision)
	h, err := NewHLL(precision)
	if err != nil {
		fmt.Println("Error creating HLL:", err)
		return
	}
	hyperLogLog = h
	fmt.Println("HyperLogLog created successfully.")
}

func addToProbabilisticStructure() {
	if hyperLogLog == nil {
		fmt.Println("Probabilistic structure not created yet.")
		return
	}
	fmt.Println("Enter a value to add to the HyperLogLog structure:")
	var value string
	fmt.Scan(&value)
	hyperLogLog.Add(value)
	fmt.Println("Value added to HyperLogLog.")
}

func queryProbabilisticStructure() {
	if hyperLogLog == nil {
		fmt.Println("Probabilistic structure not created yet.")
		return
	}
	estimate := hyperLogLog.Estimate()
	fmt.Printf("Estimated cardinality: %.2f\n", estimate)
}
