package kvengine

import (
	"fmt"
	"napredni/blockmanager"
	"napredni/cache"
	"napredni/config"
	"napredni/memtable"
	"napredni/ratelimiter"
	"napredni/sstable"
	"napredni/wal"
	"os"
	"path/filepath"
	"time"
)

// Engine predstavlja celu "bazicu" - cuva sve potrebne delove sistema
type Engine struct {
	Memtables []memtable.MemtableInterface // aktivna Memtable u RAM-u
	DataPath  string                       // putanja ka folderu sa SSTable-ovima
	WalWriter *wal.Writer                  // WAL zapisivac
	walDir    string                       // folder gde se nalaze WAL fajlovi
	memCap    int                          // kapacitet Memtable
	Cache     *cache.LRUCache              // kes
	PutCount  int                          // za ispis informacija o bazi
	GetCount  int                          // -||-

	RateLimiter       *ratelimiter.TokenBucket   // rejt limiter
	BlockManager      *blockmanager.BlockManager // block manager, za block cache
	walSegmentCounter int
}

// NewEngine pravi novi Engine sa prosledjenim podacima
func NewEngine(memCap int, walDir string, sstableDir string) *Engine {
	var mt memtable.MemtableInterface
	switch config.Current.MemtableType {
	case "hashmap":
		mt = memtable.NewHashMapMemtable(memCap)
	case "skiplist":
		mt = memtable.NewSkipListMemtable(16, 0.5)
	default:
		panic("nepoznat tip memtable")
	}

	bm := blockmanager.NewBlockManager(config.Current.BlockSizeKBK, config.Current.CacheCapacity)

	// Provera da li postoji folder za WAL
	err := os.MkdirAll(walDir, os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("ne mogu da napravim WAL folder: %v", err))
	}

	err = os.MkdirAll(sstableDir, os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("Ne mogu da napravim SSTable folder: %v", err))
	}

	/*walCounter := wal.FindMaxSegmentIndex(walDir) + 1
	// 2. Inicijalizuj WAL wirter
	w, err := wal.NewWriter(walDir, config.Current.WALSegmentSize, bm) // segment size moze biti hardkodiran za sada
	if err != nil {
		panic(fmt.Sprintf("ne mogu da inicijalizujem WAl: %v", err))
	}
	w.SetCurrentIndex(walCounter)*/

	// 2. Inicijalizuj WAL writer (on automatski nastavlja na nepopunjen segment)
	w, err := wal.NewWriter(walDir, config.Current.WALSegmentSize, bm)
	if err != nil {
		panic(fmt.Sprintf("ne mogu da inicijalizujem WAL: %v", err))
	}

	// Preuzmi koji je index aktivnog segmenta
	walCounter := w.GetCurrentIndex()

	// ako ne postoji snapshot - uradi replay wal
	snapshotPath := filepath.Join("data", "memtable.snapshot")
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		fmt.Println(" Snapshot nije pronadjen, pokrecem Replay WAL...")
		if err := wal.ReplayWAL(bm, walDir, mt); err != nil {
			fmt.Println(" Greska pri Replay WAL:", err)
		}
	} else {
		fmt.Println(" Snapshot postoji, preskacem WAL")
	}

	// Ucitaj stanje token bucket-a
	rateLimiterPath := filepath.Join(sstableDir, "..", "ratelimit.bucket")
	var rl *ratelimiter.TokenBucket

	// Proveri da li fajl postoji
	if _, err := os.Stat(rateLimiterPath); os.IsNotExist(err) {
		// Fajl NE postoji ➔ pravi default TokenBucket
		rl = ratelimiter.NewTokenBucket(100, 1000) // 100 tokens, refill na 1s
		fmt.Println("Postavljen default token bucket (100 tokens, 1s refill).")
	} else {
		// Fajl POSTOJI ➔ pokusaj da ucitas
		rl, err = ratelimiter.LoadFromFile(rateLimiterPath)
		if err != nil {
			fmt.Println(" ne mogu da ucitam token bucket:", err)
			// Ako je fajl tu, ali ostecen ili fail, pravi default
			rl = ratelimiter.NewTokenBucket(100, 1000)
			fmt.Println("Postavljen default token bucket (100 tokens, 1s refill).")
		} else {
			fmt.Println(" Token bucket uspesno ucitan iz fajla.")
		}
	}

	return &Engine{
		Memtables:         []memtable.MemtableInterface{mt}, // pravimo slice sa jednim aktivnim mt
		DataPath:          sstableDir,
		WalWriter:         w,
		walDir:            walDir,
		memCap:            memCap,
		Cache:             cache.NewLRUCache(100),
		RateLimiter:       rl,
		PutCount:          0,
		GetCount:          0,
		BlockManager:      bm,
		walSegmentCounter: walCounter,
	}
}

// E sad ovde isto ne radimo mi nikad valjda sa samo jednim Memtable-om, u interface.go mi imamo zapravo []memtable-a
// Zasto? Ne znam! Uglavnom samo jedan Memtable moze biti aktivan i u njega se upisuje i iz njega se cita, cim broj podataka u memtable bude prevelik desava se flush i taj memtable valjda postaje READ-ONLY
// I mi imamo samo jedan aktivan write-read memtable, dok je ostalih n-1 samo read.
// Zato u funkcijama imamo Memtables[0], a ne obican Memtable

func (e *Engine) Put(key string, value []byte) error {
	if !e.RateLimiter.Allow() {
		return fmt.Errorf("previse zahteva!")
	}

	fmt.Printf(" Trenutna veličina Memtable pre unosa '%s': %d\n", key, e.Memtables[0].Size())

	// Ako RW Memtable pun
	if e.Memtables[0].Size()+1 > e.memCap {
		fmt.Println(">> Memtable pun - promocija u read-only i kreiranje nove")

		oldMemtable := e.Memtables[0]
		currentSegment := e.WalWriter.GetCurrentSegmentPath()
		oldMemtable.SetSegmentPath(currentSegment)

		fmt.Printf(" Promovisem Memtable u RO — segmentPath: %s\n", currentSegment)

		e.Memtables = append(e.Memtables, oldMemtable) // Promoviši u read-only

		// Napravi novi prazan RW Memtable
		var newMemtable memtable.MemtableInterface
		switch config.Current.MemtableType {
		case "hashmap":
			newMemtable = memtable.NewHashMapMemtable(e.memCap)
		case "skiplist":
			newMemtable = memtable.NewSkipListMemtable(16, 0.5)
		default:
			panic("Nepoznat tip Memtable!")
		}
		e.Memtables[0] = newMemtable
	}

	// Ako imamo previse Memtable-ova, FLUSH
	if len(e.Memtables) > config.Current.MemtableMaxTables {
		fmt.Println(">> Previse memtable-ova, flushujemo najstariji!")
		toFlush := e.Memtables[1] // bilo len(e.Memtables)-1

		timestamp := time.Now().UnixNano()
		sstableDir := filepath.Join(e.DataPath, fmt.Sprintf("sstable_L%d_%d", 0, timestamp))

		err := os.MkdirAll(sstableDir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("greška pri pravljenju SSTable foldera: %v", err)
		}

		err = toFlush.FlushToSSTable(sstableDir, e.BlockManager)
		if err != nil {
			return fmt.Errorf("greška pri flush-u Memtable u SSTable: %v", err)
		}

		segmentPath := toFlush.GetSegmentPath()
		fmt.Printf(" Brisem WAL segment: %s\n", segmentPath)

		if err := os.Remove(segmentPath); err != nil {
			fmt.Printf(" Greska pri brisanju WAL segmenta %s: %v\n", segmentPath, err)
		}

		// Smanji listu Memtables
		//e.Memtables = e.Memtables[:len(e.Memtables)-1]

		e.Memtables = append(e.Memtables[:1], e.Memtables[2:]...)

		// Opcionalno: pokreni AutoCompact
		err = sstable.AutoCompact(e.DataPath, e.BlockManager)
		if err != nil {
			return err
		}
	}

	// 1. Upis u WAL
	record := wal.Record{
		Timestamp: uint64(time.Now().UnixNano()),
		Tombstone: false,
		Key:       []byte(key),
		Value:     value,
	}
	err := e.WalWriter.Write(record)
	if err != nil {
		return fmt.Errorf("greška pri pisanju u WAL: %v", err)
	}

	// 2. Upis u RW Memtable
	e.Memtables[0].Put(key, value)

	// 3. Upis u Cache
	e.Cache.Put(key, value)

	e.PutCount++
	return nil
}

// Get pokusava da pronadje kljuc - prvo u Memtable, pa u SSTable
func (e *Engine) Get(key string) ([]byte, bool) {
	if !e.RateLimiter.Allow() {
		fmt.Println("previse zahteva!")
		return nil, false
	}

	fmt.Printf(" GET kljuc: %s\n", key)

	val, found := e.Cache.Get(key)
	if found {
		fmt.Println("Kes pogodak za:", key)
		return val, true
	}

	// trazenje kroz sve memtable
	for _, mt := range e.Memtables {
		value, found := mt.Get(key)
		if found {
			if value == nil {
				//tombstone
				return nil, false
			}
			// nasli smo ga
			e.Cache.Put(key, value)
			e.GetCount++
			return value, true
		}
	}

	value, found := sstable.FastGetFromSSTablesWithBlocks(e.DataPath, key, e.BlockManager) // ➔ nova funkcija
	if found {
		e.GetCount++
		e.Cache.Put(key, value)
	}
	return value, found
}

func (e *Engine) Delete(key string) error {
	if !e.RateLimiter.Allow() {
		return fmt.Errorf("previse zahteva")
	}

	// 4. Provera da li Memtable treba da se zameni
	if e.Memtables[0].Size()+1 > e.memCap {
		fmt.Println(">> Memtable pun - promovisem u read-only pravim novu (delete)!")

		oldMemtable := e.Memtables[0]
		currentSegment := e.WalWriter.GetCurrentSegmentPath()
		oldMemtable.SetSegmentPath(currentSegment)

		fmt.Printf(" Promovisem Memtable u RO — segmentPath: %s\n", currentSegment)

		e.Memtables = append(e.Memtables, oldMemtable)

		// Napravi novi RW Memtable
		var newMemtable memtable.MemtableInterface
		switch config.Current.MemtableType {
		case "hashmap":
			newMemtable = memtable.NewHashMapMemtable(e.memCap)
		case "skiplist":
			newMemtable = memtable.NewSkipListMemtable(16, 0.5)
		default:
			panic("Nepoznat tip Memtable!")
		}
		e.Memtables[0] = newMemtable
	}

	// 1. Upis tombstone zapisa u WAL
	record := wal.Record{
		Timestamp: uint64(time.Now().UnixNano()),
		Tombstone: true,
		Key:       []byte(key),
		Value:     nil,
	}

	err := e.WalWriter.Write(record)
	if err != nil {
		return fmt.Errorf("greska pri pisanju u WAL (delete): %v", err)
	}

	// 2. Upis tombstone u aktivni Memtable
	e.Memtables[0].Delete(key)

	// 3. Ukloni iz Cache
	e.Cache.Remove(key)

	// 5. Provera da li ima previse Memtables → Flush
	if len(e.Memtables) > config.Current.MemtableMaxTables {
		fmt.Println(">> Previse memtable-ova, flushujem poslednju (delete)")

		toFlush := e.Memtables[len(e.Memtables)-1]

		timestamp := time.Now().UnixNano()
		sstableDir := filepath.Join(e.DataPath, fmt.Sprintf("sstable_L%d_%d", 0, timestamp))

		err := os.MkdirAll(sstableDir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("greška pri kreiranju SSTable foldera: %v", err)
		}

		err = toFlush.FlushToSSTable(sstableDir, e.BlockManager)
		if err != nil {
			return fmt.Errorf("greška pri flush-u Memtable (Delete): %v", err)
		}

		segmentPath := toFlush.GetSegmentPath()
		fmt.Printf(" Brisem WAL segment: %s\n", segmentPath)

		if err := os.Remove(segmentPath); err != nil {
			fmt.Printf(" Greska pri brisanju WAL segmenta %s: %v\n", segmentPath, err)
		}

		// Smanji slice
		//e.Memtables = e.Memtables[:len(e.Memtables)-1]
		e.Memtables = append(e.Memtables[:1], e.Memtables[2:]...)

		// Opcionalno pokreni kompakciju
		err = sstable.AutoCompact(e.DataPath, e.BlockManager)
		if err != nil {
			return err
		}
	}

	return nil
}
