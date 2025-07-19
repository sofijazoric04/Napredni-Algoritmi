package memtable

import (
	"encoding/gob"
	"fmt"
	"os"
)

// Snapshot ima ulogu u 'zamrzavanju' stanja memtable-a prilikom gasenja baze
// Rekli smo da ako nesto zapisujemo sve ide u WAL + Memtable, i uradimo samo EXIT, pri novom pokretanju baze zove se funkcija ReplayWAL i restaurira wal podatke u memtable
// Medjutim ako pre exit-a iz baze mi uradimo SNAPSHOT_SAVE cuvamo njeno stanje, pri novom ulasku u bazu pozovemo SNAPSHOT_LOAD i dobijamo podatke, nema ReplayWAL-a
// Isto kod skiplist_snapshot.go

// SaveSnapshot snima stanje cele Memtable mape u fajl
func (m *HashMapMemtable) SaveSnapshot(path string) error {
	file, err := os.Create(path)

	if err != nil {
		return fmt.Errorf("ne mogu da kreiram snapshot fajl: %v", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)

	// Napravis slice svih unosa iz mape koje zelimo da sacuvamo
	var entries []SnapshotEntry
	for k, v := range m.data {
		entries = append(entries, SnapshotEntry{
			Key:       k,
			Value:     v.Value,
			Tombstone: v.Tombstone,
		})
	}

	// Snimi slice u fajl
	if err := encoder.Encode(entries); err != nil {
		return fmt.Errorf("ne mogu da serijalizujem snapshot: %v", err)
	}
	return nil
}

// LoadSnapshot ucitava mapu iz snapshot fajla
func (m *HashMapMemtable) LoadSnapshot(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("ne mogu da otvorim snapshot: %v", err)
	}
	defer file.Close()

	var entries []SnapshotEntry
	decoder := gob.NewDecoder(file)

	if err := decoder.Decode(&entries); err != nil {
		return fmt.Errorf("ne mogu da dekodiram snapshot: %v", err)
	}

	// ocistimo trenutnu mapu i napunimo iz fajla
	m.data = make(map[string]Entry)
	for _, e := range entries {
		m.data[e.Key] = Entry{
			Value:     e.Value,
			Tombstone: e.Tombstone,
		}
	}
	return nil

}
