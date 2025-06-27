package blockmanager

import (
	"fmt"
	"os"
	"sync"
)

// BlockID predstavlja identifikaciju bloka
// Svaki blok ima svoj broj
type BlockID struct {
	Path string // putanja do fajla
	Num  int64  // broj bloka
}

// BlockManager organizuje citanje/pisanje blokova sa kesiranjem
type BlockManager struct {
	blockSize int         // velicina bloka (npr 4KB)
	cache     *BlockCache // kesira procitanje blokove
	mu        sync.Mutex
}

// NewBlockManager kreira novi BlockManager
func NewBlockManager(blockSizeKB, cacheCapacity int) *BlockManager {
	return &BlockManager{
		blockSize: blockSizeKB * 1024,           // npr 4KB
		cache:     NewBlockCache(cacheCapacity), // koliko blokova moze da stane u memoriju
	}
}

// ReadBlock čita blok iz kesa ili sa diska
func (bm *BlockManager) ReadBlock(id BlockID) ([]byte, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// proveri cache
	if data, ok := bm.cache.Get(id); ok {
		return data, nil
	}

	// ako nema u cache citaj sa diska
	f, err := os.Open(id.Path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// izracunaj offset
	offset := id.Num * int64(bm.blockSize) // offset za koliko se treba pomeriti i uctitali blok, npr ako citamo prvi blok, to je blok0, 0 * 4kb je 0 to je prvi blok, blok 2, 2 * 4kb citamo posle 8kb blok

	// citaj blok
	buf := make([]byte, bm.blockSize)
	n, err := f.ReadAt(buf, offset) // readAt cita od offseta i to za duzinu buf
	if err != nil && n == 0 {
		return nil, err
	}

	// upamti blok u cache
	bm.cache.Put(id, buf)
	return buf, nil
}

// WriteBlock upisuje blok u fajl i azurira kes
func (bm *BlockManager) WriteBlock(id BlockID, data []byte) error {
	// garantujemo da data stane u jedan blok
	if len(data) > bm.blockSize {
		return fmt.Errorf("data size exceeds block size")
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	// otvaramo fajl, ako ne postoji napravimo ga
	// 0644 ➔ standardne dozvole za fajl (vlasnik može da čita/piše, ostali samo da čitaju).
	f, err := os.OpenFile(id.Path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	//Blok broj × veličina bloka = bajt gde počinje blok.
	offset := id.Num * int64(bm.blockSize)

	_, err = f.WriteAt(data, offset)
	if err != nil {
		return err
	}

	bm.cache.Put(id, data)
	return nil
}

/*
+-------------------------+-------------------------+-------------------------+
| Blok 0: Data             | Blok 1: Data             | Blok 2: Data             |
| CRC | TS | Tomb | Key/Val| CRC | TS | Tomb | Key/Val| CRC | TS | Tomb | Key/Val|
|...                        |...                        |...                        |
+-------------------------+-------------------------+-------------------------+

*/

func (bm *BlockManager) BlockSize() int {
	return bm.blockSize
}
