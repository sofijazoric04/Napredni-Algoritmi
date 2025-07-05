package cli_simhash

import (
	"fmt"
	"kvstore/simhash"
	"os"
	"path/filepath"
	"strings"
)

const basePath = "data/probabilistic"

func Handle(input string) {
	args := strings.Fields(input)
	if len(args) < 2 {
		fmt.Println("Neispravna SIMHASH komanda")
		return
	}

	cmd := strings.ToUpper(args[0])
	name := args[1]
	filePath := filepath.Join(basePath, "simhash_"+name+".simhash")

	switch cmd {
	case "SIMHASH_NEW":
		if len(args) != 3 {
			fmt.Println("Koriscenje: SIMHASH_NEW ime fajl.txt")
			return
		}

		content, err := os.ReadFile(args[2])
		if err != nil {
			fmt.Println("Greska pri citanju fajla:", err)
			return
		}

		hash := simhash.NewSimHashFromText(string(content))
		os.MkdirAll(basePath, os.ModePerm)
		if err := hash.SaveToFile(filePath); err != nil {
			fmt.Println(" Greška pri snimanju fingerprinta:", err)
			return
		}
		fmt.Println(" Fingerprint sačuvan za:", name)

	case "SIMHASH_DISTANCE":
		if len(args) != 3 {
			fmt.Println(" Korišćenje: SIMHASH_DISTANCE ime1 ime2")
			return
		}

		h1Path := filepath.Join(basePath, "simhash_"+args[1]+".simhash")
		h2Path := filepath.Join(basePath, "simhash_"+args[2]+".simhash")

		h1, err1 := simhash.LoadFromFile(h1Path)
		h2, err2 := simhash.LoadFromFile(h2Path)

		if err1 != nil || err2 != nil {
			fmt.Println(" Greška pri učitavanju fingerprinta.")
			return
		}

		distance := simhash.HammingDistance(h1, h2)
		fmt.Printf(" Hamming distanca između '%s' i '%s' iznosi: %d\n", args[1], args[2], distance)

	default:
		fmt.Println(" Nepoznata SIMHASH komanda:", cmd)
	}
}
