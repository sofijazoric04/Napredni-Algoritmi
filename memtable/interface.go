package memtable

import "napredni/blockmanager"

// MemtableInterface opisuje ponasanje od bilo koje Memtable strukture
// To je isto ponasanje kao u C++ znaci imamo .h i .cpp fajlove, u .h se upisuju samo potpisi funkcija, 
// a u .cpp se implementira telo funkcije tako je i sa ovim interface-om
// ovo napravljeno jer podrzavamo dve razlicite implementacije memtable strukture
type MemtableInterface interface {
	Put(key string, value []byte)                                    // ubacivanje vrednosti
	Get(key string) ([]byte, bool)                                   // dobavljanje vrednosti
	Delete(key string)                                               // logicko brisanje
	FlushToSSTable(path string, bm *blockmanager.BlockManager) error // prebacivanje na disk
	Size() int                                                       // trenutna velicina
	RangeScan(from, to string) map[string][]byte

	GetSegmentPath() string
	SetSegmentPath(path string)

	SaveSnapshot(path string) error
	LoadSnapshot(path string) error
}
