package cli_bloomfilter

import (
	"fmt"
	"napredni/bloomfilter"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const basePath = "data/probabilistic"

func Handle(input string) {
	args := strings.Fields(input)
	if len(args) < 2 {
		fmt.Println("Neispravna komanda")
		return
	}

	cmd := strings.ToUpper(args[0])
	name := args[1]
	filePath := filepath.Join(basePath, "bf_"+name+".bloom")

	switch cmd {
	case "BF_NEW":
		if len(args) != 4 {
			fmt.Println("Koriscenje: BF_NEW naziv brojElem stopaGreske")
			return
		}

		n, _ := strconv.Atoi(args[2])
		p, _ := strconv.ParseFloat(args[3], 64)
		bf := bloomfilter.NewBloomFilter(n, p)

		// Kreiraj folder ako ne postoji
		os.MkdirAll(basePath, os.ModePerm)
		err := bf.SaveToFile(filePath)
		if err != nil {
			fmt.Println("Greska pri snimanju:", err)
		} else {
			fmt.Println("Bloom filter", name, "je kreiran")
		}

	case "BF_ADD":
		if len(args) != 3 {
			fmt.Println("Koriscenje: BF_ADD naziv kljuc")
			return
		}

		bf, err := bloomfilter.LoadFromFile(filePath)
		if err != nil {
			fmt.Println("Greska pri ucitavanju:", err)
			return
		}
		fmt.Println("🧪 Pre dodavanja: aktivnih bitova =", countTrueBits(bf.Bitset))
		bf.Add(args[2])
		fmt.Println("🧪 Posle dodavanja:", countTrueBits(bf.Bitset))

		err = bf.SaveToFile(filePath)
		if err != nil {
			fmt.Println(" Greška pri snimanju:", err)
			return
		}

		// 🚨 Dodajemo proveru odmah nakon snimanja
		bf2, err := bloomfilter.LoadFromFile(filePath)
		if err != nil {
			fmt.Println(" Greška pri re-učitavanju:", err)
			return
		}
		fmt.Println("🧪 Posle ponovnog učitavanja:", countTrueBits(bf2.Bitset))

	case "BF_QUERY":
		if len(args) != 3 {
			fmt.Println("Koriscenje: BF_QUERY naziv kljuc")
			return
		}

		bf, err := bloomfilter.LoadFromFile(filePath)
		if err != nil {
			fmt.Println("Greska pri ucitavanju:", err)
			return
		}
		if bf.MayContain(args[2]) {
			fmt.Println("Mozda postoji:", args[2])
		} else {
			fmt.Println("Sigurno ne postoji:", args[2])
		}

	case "BF_DELETE":
		err := os.Remove(filePath)
		if err != nil {
			fmt.Println("Greska pri brisanju:", err)
		} else {
			fmt.Println("Bloom filter obrisan:", err)
		}

	default:
		fmt.Println("Nepoznata komanda:", cmd)

	}

}

func countTrueBits(bits []bool) int {
	count := 0
	for _, b := range bits {
		if b {
			count++
		}
	}
	return count
}
