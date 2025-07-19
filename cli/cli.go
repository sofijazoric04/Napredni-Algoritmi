package cli

import (
	"bufio"
	"fmt"
	"napredni/cli_bloomfilter"
	"napredni/cli_cmsketch"
	"napredni/cli_hll"
	"napredni/cli_simhash"
	"napredni/config"
	"napredni/kvengine"
	"napredni/ratelimiter"
	"napredni/sstable"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Start pokrece komandnu petlju
func Start(engine *kvengine.Engine) {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Key-Value Store CLI - upisi HELP za komande")
	var prefixIterator *kvengine.PrefixIterator
	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		// Ako je unos prazan preskoci
		if input == "" {
			continue
		}

		// Parsiraj komandne reci
		args := strings.Fields(input)
		cmd := strings.ToUpper(args[0])

		// Ako je komanda za bloom filter poziva poseban handler
		if strings.HasPrefix(cmd, "BF_") {
			cli_bloomfilter.Handle(input)
			continue
		}

		// Isto za CMS
		if strings.HasPrefix(strings.ToUpper(args[0]), "CMS_") {
			cli_cmsketch.Handle(input)
			continue
		}

		// HLL
		if strings.HasPrefix(strings.ToUpper(args[0]), "HLL_") {
			cli_hll.Handle(input)
			continue
		}

		// SIMHASH
		if strings.HasPrefix(strings.ToUpper(args[0]), "SIMHASH_") {
			cli_simhash.Handle(input)
			continue
		}

		fmt.Println("args:", args)
		switch cmd {
		case "SET_RATE_LIMIT":
			if len(args) != 3 {
				fmt.Println("Koriscenje: SET_RATE_LIMIT <maxTokens> <refillMs>")
				break
			}

			maxTokens, err1 := strconv.Atoi(args[1])
			refillMs, err2 := strconv.Atoi(args[2])
			if err1 != nil || err2 != nil {
				fmt.Println("parametri moraju biti brojevi")
				break
			}

			rateLimiter := ratelimiter.NewTokenBucket(maxTokens, refillMs)
			engine.RateLimiter = rateLimiter

			// Serijalizuje stanje u fajl
			path := filepath.Join(engine.DataPath, "..", "ratelimit.bucket")
			err := rateLimiter.SaveToFile(path)
			if err != nil {
				fmt.Println("ne mogu da sacuvam tb:", err)
			} else {
				fmt.Println("rate limiter podesen:", maxTokens, "tokena,", refillMs, "ms refill")
			}

		case "PUT":

			if len(args) != 3 {
				fmt.Println("Koriscenje: PUT kljuc vrednost")
				continue
			}
			err := engine.Put(args[1], []byte(args[2]))
			if err != nil {
				fmt.Println("Greska pri upisu: ", err)
			} else {
				fmt.Println("Uspesno upisano.")
			}

		case "GET":

			// rl
			if engine.RateLimiter != nil && !engine.RateLimiter.Allow() {
				fmt.Println("Prekoračen broj zahteva. Sačekaj refill.")
				break
			}

			if len(args) != 2 {
				fmt.Println("Koriscenje: GET kljuc")
				continue
			}
			val, found := engine.Get(args[1])
			if found {
				fmt.Printf("Vrednost za %s je: %s\n", args[1], val)
			} else {
				fmt.Println("Kljuc nije pronadjen")
			}

		case "DELETE":

			// rl
			if engine.RateLimiter != nil && !engine.RateLimiter.Allow() {
				fmt.Println("Prekoračen broj zahteva. Sačekaj refill.")
				break
			}

			if len(args) != 2 {
				fmt.Println("Koriscenje: DELETE kljuc")
				continue
			}
			err := engine.Delete(args[1])
			if err != nil {
				fmt.Println("Greska pri brisanju: ", err)
			} else {
				fmt.Println("Kljuc logicki obrisan")
			}

		case "RANGE":
			if len(args) != 3 {
				fmt.Println("Koriscenje: RANGE <od_kljuca> <do_kljuca>")
				break
			}
			from := args[1]
			to := args[2]

			// Izvrsava RangeScan nad memtable
			results := engine.RangeScan(from, to)
			if len(results) == 0 {
				fmt.Println("Nema rezultata u opsegu")
			} else {
				fmt.Println("Rezultati:")
				var keys []string
				for k := range results {
					keys = append(keys, k)
				}

				// Sortira ključeve
				sort.Strings(keys)

				// Ispisuje po redu
				for _, k := range keys {
					fmt.Printf("%s - %s\n", k, results[k])
				}
			}

		case "RANGE_ALL":
			results := engine.RangeScan("", "zzzzzz")
			if len(results) == 0 {
				fmt.Println("Memtable je trnutno prazna")
			} else {
				fmt.Println("Svi zapisi u bazi:")
				var keys []string
				for k := range results {
					keys = append(keys, k)
				}

				// Sortira ključeve
				sort.Strings(keys)

				// Ispisuje po redu
				for _, k := range keys {
					fmt.Printf("%s - %s\n", k, results[k])
				}
			}

		case "RANGE_SCAN":
			if len(args) != 5 {
				fmt.Println("Koriscenje: RANGE_SCAN <from> <to> <pageNumber> <pageSize>")
				break
			}
			from := args[1]
			to := args[2]
			pageNum, _ := strconv.Atoi(args[3])
			pageSize, _ := strconv.Atoi(args[4])
			results := engine.RangeScanPaginated(from, to, pageNum, pageSize)
			if len(results) == 0 {
				fmt.Println("nema rezultata za ovu stranicu")
			} else {
				fmt.Println("Rezultati:")
				var keys []string
				for k := range results {
					keys = append(keys, k)
				}
				// Sortira ključeve
				sort.Strings(keys)

				// Ispisuje po redu
				for _, k := range keys {
					fmt.Printf("%s - %s\n", k, results[k])
				}
			}

		case "PREFIX_SCAN":
			if len(args) != 4 {
				fmt.Println("Koriscenje: PREFIX_SCAN <prefix> <pageNumber> <pageSize>")
				break
			}
			prefix := args[1]
			pageNumber, err1 := strconv.Atoi(args[2])
			pageSize, err2 := strconv.Atoi(args[3])
			if err1 != nil || err2 != nil {
				fmt.Println("neispravni parametri za broj stranica ili velicinu")
				return
			}

			results := engine.PrefixScanPagination(prefix, pageNumber, pageSize)

			if len(results) == 0 {
				fmt.Println("Nema rezultata za zadati prefiks")
				break
			}

			fmt.Println("Kljucevi sa prefiksom:", prefix)
			var keys []string
			for k := range results {
				keys = append(keys, k)
			}

			// Sortira ključeve
			sort.Strings(keys)

			// Ispisuje po redu
			for _, k := range keys {
				fmt.Printf("%s - %s\n", k, results[k])
			}

		case "PREFIX_ITERATE":
			if len(args) != 2 {
				fmt.Println("Koriscenje: PREFIX_ITERATE <prefix>")
				break
			}
			prefix := args[1]
			prefixIterator = engine.NewPrefixIterator(prefix)
			fmt.Println("Prefix iterator kreiran za prefiks:", prefix)

		case "RANGE_ITERATOR":
			if len(args) < 3 {
				fmt.Println("Koriscenje: RANGE_ITERATOR <from> <to>")
				break
			}

			from := args[1]
			to := args[2]
			it := engine.NewRangeIterator(from, to)

			fmt.Println("Pokrenut RANGE_ITERATOR. Kucaj NEXT za sledeci, STOP za kraj")
		iteratorLoop:
			for {
				fmt.Println("> ")
				reader := bufio.NewReader(os.Stdin)
				command, _ := reader.ReadString('\n')
				command = strings.TrimSpace(command)

				switch strings.ToUpper(command) {
				case "NEXT":
					k, v, ok := it.Next()
					if !ok {
						fmt.Println("nema vise lemenata")
					} else {
						fmt.Printf(" %s - %s\n", k, string(v))
					}
				case "STOP":
					it.Stop()
					fmt.Println("iterator zaustavljen")
					break iteratorLoop
				default:
					fmt.Println("nepoznata komanda, koristi NEXT ili STOP")
				}
			}

		case "NEXT":
			if prefixIterator == nil {
				fmt.Println("Nema aktivnog prefiks iteratora")
				break
			}
			key, value := prefixIterator.Next()
			if key == "" {
				fmt.Println("Nema vise elemenata")
			} else {
				fmt.Printf(" %s - %s\n", key, string(value))
			}

		case "STOP":
			if prefixIterator == nil {
				fmt.Println("Nema aktivnog prefiks iteratora")
				break

			}
			prefixIterator.Stop()
			fmt.Println("Prefiks iterator zaustavljen")

		case "MERGE":
			fmt.Println(" Pokrećem kompaktiranje SSTable-ova...")

			err := sstable.CompactSSTables(engine.DataPath, engine.BlockManager)
			if err != nil {
				fmt.Println(" Greška pri kompaktiranju:", err)
			} else {
				fmt.Println(" Kompaktiranje završeno.")
			}

		case "SNAPSHOT_SAVE":
			err := engine.Memtables[0].SaveSnapshot("data/memtable.snapshot")
			if err != nil {
				fmt.Println(" Greska pri snimanju snapshot-a:", err)
			} else {
				fmt.Println(" Snapshot uspesno snimljen.")
			}
		case "SNAPSHOT_LOAD":
			err := engine.Memtables[0].LoadSnapshot("data/memtable.snapshot")
			if err != nil {
				fmt.Println(" Greska pri ucitavanju snapshot-a:", err)
			} else {
				fmt.Println(" Snapshot uspesno ucitan.")
			}

		case "MERKLE_VALIDATE":
			if len(args) != 2 {
				fmt.Println("Koriscenje: MERKLE_VALIDATE <sstable_folder_name>")
				break
			}
			folderName := args[1]
			fullPath := filepath.Join(engine.DataPath, folderName)

			valid, err := sstable.ValidateMerkleTree(fullPath, engine.BlockManager)
			if err != nil {
				fmt.Println("Greska pri validaciji Merkle stabla:", err)
			} else if valid {
				fmt.Println(" Merkle stablo validno! Nema izmena.")
			} else {
				fmt.Println(" Merkle stablo NIJE validno! Detektovana izmena.")
			}

		case "STATS":
			fmt.Println("Statistika baze:")
			fmt.Println(". Broj kljuceva u memtable:", engine.Memtables[0].Size())
			fmt.Println(". Broj SSTable fajlova:", countSSTables(engine.DataPath))
			fmt.Println(". Ukupno GET poziva:", engine.GetCount)
			fmt.Println(". Ukupno PUT poziva:", engine.PutCount)

		case "WAL_STATE":
			name, count := engine.WalWriter.StateInfo()
			fmt.Printf("Aktivni WAL fajl: %s\n, broj zapisa: %d\n", name, count)

		case "MEMTABLE_STATE":
			fmt.Println("Stanje memtable-a")
			fmt.Println(". Tip:", config.Current.MemtableType)
			fmt.Println(". Broj kljuceva:", engine.Memtables[0].Size())
			// Prikaz nekoliko parova
			fmt.Println("Primer unosa:")
			all := engine.Memtables[0].RangeScan("", "zzzzzz")
			fmt.Println("Prvih 3 kljuca u memtable:")
			i := 0
			for k, v := range all {
				if i >= 5 {
					break
				}
				fmt.Printf(". %s - %s\n", k, v)
				i++
			}

		case "CACHE_STATE":
			fmt.Println("LRU Cache stanje:")
			for k, v := range engine.Cache.Items() {
				fmt.Printf(" %s - %s\n", k, v)
			}

		case "SHOW_CONFIG":
			fmt.Println("Trenutna konfiguracija baze:")
			fmt.Println(". Tip memtable:", config.Current.MemtableType)
			fmt.Println(". Max broj unosa u Memtable:", config.Current.MemtableMaxEntries)
			fmt.Println(". Velicina WAL segmenta:", config.Current.WALSegmentSize)
			fmt.Println(". Velicina bloka:", config.Current.BlockSizeKBK)
			fmt.Println(". Cache kapacitet:", config.Current.CacheCapacity)

		case "HELP":
			fmt.Println("# Dostupne komande:")
			fmt.Println("PUT ključ vrednost  - dodaj ili ažuriraj podatak")
			fmt.Println("GET ključ            - dohvat vrednosti za dati ključ")
			fmt.Println("DELETE ključ         - obriši ključ (logički)")
			fmt.Println("RANGE from to        - ispis kljuceva u opsegu od - do")
			fmt.Println("RANGE_ALL            - ispis svih kljuceva i vrednosti")
			fmt.Println("RANGE_SCAN from to pageNumber pageSize     - ispis kljuceva u opsegu po stranici")
			fmt.Println("PREFIX_SCAN prefix   - isto kao RANGE_SCAN samo za prefiks")
			fmt.Println("PREFIX_ITERATE prefix       - isto kao PREFIX_SCAN samo postoje pozivi NEXT, I STOP dokle god ima rezultata")
			fmt.Println("RANGE_ITERATOR from to      - isto kao PREFIX_SCAN samo sto se odnosi na ceo kljuc, a ne prefiks")
			fmt.Println("MERGE                - kompaktiranje SSTable")
			fmt.Println("SNAPSHOT_SAVE ime    - 'zamrzavanje' trenutne baze, cuvanje vrednosti")
			fmt.Println("SNAPSHOT_LOAD ime    - ucitava prethodno sacuvani snapshot sa informacijama")
			fmt.Println("HELP                 - pomoć")
			fmt.Println("EXIT                 - izlaz")

		case "EXIT":
			fmt.Println(" Zatvaranje baze. Doviđenja!")
			engine.FlushAllMemtables()
			return

		default:
			fmt.Println(" Nepoznata komanda. Ukucaj HELP za pomoć.")
		}
	}

}

// pomocna funkcija
func countSSTables(path string) int {
	files, err := os.ReadDir(path)
	if err != nil {
		return 0
	}

	count := 0
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "sstable_") {
			count++
		}
	}
	return count
}
