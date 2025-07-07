package cli_cmsketch

import (
	"fmt"
	"napredni/cms"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const basePath = "data/probabilistic"

func Handle(input string) {
	args := strings.Fields(input)
	if len(args) < 2 {
		fmt.Println("Neispravna CMS komanda")
		return
	}

	cmd := strings.ToUpper(args[0])
	name := args[1]
	filePath := filepath.Join(basePath, "cms_"+name+".cms")

	switch cmd {
	case "CMS_NEW":
		if len(args) != 4 {
			fmt.Println("Koriscenje: CMS_NEW naziv epsilon delta")
			return
		}

		epsilon, _ := strconv.ParseFloat(args[2], 64)
		delta, _ := strconv.ParseFloat(args[3], 64)

		sketch := cms.NewCMS(epsilon, delta)

		os.MkdirAll(basePath, os.ModePerm)
		err := sketch.SaveToFile(filePath)

		if err != nil {
			fmt.Println("Greska pri snimanju:", err)
		} else {
			fmt.Println("CMS", name, "je kreiran")
		}

	case "CMS_ADD":
		if len(args) != 3 {
			fmt.Println("Koriscenje: CMS_ADD naziv kljuc")
			return
		}

		sketch, err := cms.LoadFromFile(filePath)
		if err != nil {
			fmt.Println("Greska pri ucitavanju:", err)
			return
		}
		sketch.Add(args[2])
		sketch.SaveToFile(filePath)
		fmt.Println("Dodat kljuc:", args[2])

	case "CMS_EST":
		if len(args) != 3 {
			fmt.Println("Koriscenje: CMS_EST naziv kljuc")
			return
		}
		sketch, err := cms.LoadFromFile(filePath)
		if err != nil {
			fmt.Println("Greska pri ucitavanju:", err)
			return
		}
		est := sketch.Estimate(args[2])
		fmt.Printf("Estimacija za '%s' : %d pojavljivanja\n", args[2], est)

	case "CMS_DELETE":
		err := os.Remove(filePath)
		if err != nil {
			fmt.Println("Greska pri brisanju:", err)
		} else {
			fmt.Println("CMS obrisan:", name)
		}

	default:
		fmt.Println("Nepoznata CMS komanda:", cmd)

	}
}
