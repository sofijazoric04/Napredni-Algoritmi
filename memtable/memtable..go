package memtable

import (
	"sync"
)

// Jedan zapis koji se cuva u Memtable
type Entry struct {
	Value     []byte // vrednost kao niz bajtova
	Tombstone bool   // true ako je obriasn (logicko brisanje)
}

// Glavna struktura za Memtable
type HashMapMemtable struct {
	data        map[string]Entry // mapa: kljuc -> Entry
	mu          sync.RWMutex     // bezbedan rad sa vise niti
	Cap         int              // kapacitet
	segmentPath string
}

// Konstruktor: pravi novu praznu memtable sa zadatim kapacitetom
func NewHashMapMemtable(capacity int) *HashMapMemtable {
	return &HashMapMemtable{
		data: make(map[string]Entry),
		Cap:  capacity,
	}
}

// Ubacuje (ili menja) zapis u Memtable
func (m *HashMapMemtable) Put(key string, value []byte) {
	m.mu.Lock() //zakljucamo mapu da bi izbegli konkurentni pristup
	defer m.mu.Unlock()

	/*m.data[key] = Entry{
		Value:     value,
		Tombstone: false,
	}*/

	if value == nil {
		m.data[key] = Entry{Tombstone: true} // ako je value nil, postavljamo Tombstone na true (logicko brisanje)
	} else {
		m.data[key] = Entry{Value: value, Tombstone: false} // postavljamo vrednost, Tombstone na false
	}
}

// Vraca vrednost ako postoji i nije obrisana
func (m *HashMapMemtable) Get(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, exists := m.data[key]
	/*if !exists || entry.Tombstone {
		return nil, false
	}

	return entry.Value, true*/

	if !exists {
		return nil, false // kljuc ne postoji
	}
	if entry.Tombstone {
		return nil, true // kljuc postoji ali je logicki obrisan
	}
	return entry.Value, true // kljuc postoji i nije obrisan, vracamo njegovu vrednost
}

// Brise zapis logicki (tombstone = true)
func (m *HashMapMemtable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = Entry{
		Tombstone: true,
	}
}

func (m *HashMapMemtable) SetSegmentPath(path string) {
	m.segmentPath = path
}

func (m *HashMapMemtable) GetSegmentPath() string {
	return m.segmentPath
}

// vraca broj zapisa u tabeli
func (m *HashMapMemtable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

// obicna provera da li je popunjen kapacitet
func (m *HashMapMemtable) IsFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data) >= m.Cap
}
