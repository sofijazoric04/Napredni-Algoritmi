package cli_hll

import (
	"fmt"
	"napredni/hyperloglog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const basePath = "data/probabilistic"

func Handle(input string) {
	args := strings.Fields(input)
	if len(args) < 2 {
		fmt.Println("Neispravna HLL komanda")
		return
	}

	cmd := strings.ToUpper(args[0])
	name := args[1]
	filePath := filepath.Join(basePath, "hll_"+name+".hll")

	switch cmd {
	case "HLL_NEW":
		if len(args) != 3 {
			fmt.Println("Koriscenje: HLL_NEW naziv p")
			return
		}
		p, err := strconv.Atoi(args[2])
		if err != nil || p < 4 || p > 16 {
			fmt.Println(" p mora biti bronj izmedju 4 i 16")
			return
		}
		os.MkdirAll(basePath, os.ModePerm)
		h := hyperloglog.NewHLL(uint8(p))
		err = h.SaveToFile(filePath)
		if err != nil {
			fmt.Println("greska pri snimanju:", err)
		} else {
			fmt.Println("HLL", name, "je kreiran")
		}

	case "HLL_ADD":
		if len(args) != 3 {
			fmt.Println("Koriscenje: HLL_ADD naziv podatak")
			return
		}
		h, err := hyperloglog.LoadFromFile(filePath)
		if err != nil {
			fmt.Println("greska pri ucitavanju:", err)
			return
		}
		h.Add(args[2])
		h.SaveToFile(filePath)
		fmt.Println("Dodat:", args[2])

	case "HLL_COUNT":
		h, err := hyperloglog.LoadFromFile(filePath)
		if err != nil {
			fmt.Println("greska pri ucitavanju:", err)
			return
		}
		count := h.Estimate()
		fmt.Printf("procena unikatnih elemenata: %0f\n", count)

	case "HLL_DELETE":
		err := os.Remove(filePath)
		if err != nil {
			fmt.Println("greska pri brisanju:", err)
		} else {
			fmt.Println("HLL obrisan:", name)
		}

	default:
		fmt.Println("Nepoznata HLL komanda:", cmd)

	}

}
