package memtable

import (
	"encoding/gob"
	"fmt"
	"os"
)

// SnapshotEntry reprezentuje jedan unos u snapshotu skip liste
type SnapshotEntry struct {
	Key       string
	Value     interface{}
	Tombstone bool
}

// SaveSnapshot za SkipListMemtable
func (s *SkipListMemtable) SaveSnapshot(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("ne mogu da kreiram snapshot fajl: %v", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)

	// Prolazimo kroz sve čvorove skip liste (level 0 je najniži nivo – pun)
	var entries []SnapshotEntry
	current := s.head.next[0] // prvi čvor posle head-a
	for current != nil {
		entries = append(entries, SnapshotEntry{
			Key:       current.key,
			Value:     current.value,
			Tombstone: current.tombstone,
		})
		current = current.next[0]
	}

	if err := encoder.Encode(entries); err != nil {
		return fmt.Errorf("ne mogu da serijalizujem snapshot: %v", err)
	}

	return nil
}

// LoadSnapshot za SkipListMemtable
func (s *SkipListMemtable) LoadSnapshot(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("ne mogu da otvorim snapshot fajl: %v", err)
	}
	defer file.Close()

	var entries []SnapshotEntry
	decoder := gob.NewDecoder(file)

	if err := decoder.Decode(&entries); err != nil {
		return fmt.Errorf("ne mogu da dekodiram snapshot: %v", err)
	}

	// Očisti listu i ubaci sve nove
	s.head = NewSkipListNode("", nil, false, s.maxLevel)
	s.level = 1
	s.size = 0

	for _, e := range entries {
		valueBytes, ok := e.Value.([]byte)
		if !ok {
			return fmt.Errorf("ne mogu da konvertujem vrednost za ključ '%s' u []byte", e.Key)
		}
		s.Put(e.Key, valueBytes)
		if e.Tombstone {
			s.Delete(e.Key)
		}
	}

	return nil
}
