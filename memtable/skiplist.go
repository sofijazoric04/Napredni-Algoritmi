package memtable

import (
	"fmt"
	"kvstore/blockmanager"
	"kvstore/sstable"
	"math/rand"
	"os"
	"time"
)

// Definicija cvora
type SkipListNode struct {
	key       string          // kljuc
	value     []byte          // vrednost u bajtima
	tombstone bool            // da li je obrisan
	next      []*SkipListNode // pokazivaci na sledeci cvor po svakom nivou
}

/*
Nivo 3: head ->    ->    ->
Nivo 2: head -> node3 -> node5
Nivo 1: head -> node1 -> node3 -> node4 -> node5 -> node7
*/

// Definicija skipliste
type SkipListMemtable struct {
	head        *SkipListNode // pocetni dummy cvor koji nema podatke
	level       int
	maxLevel    int // maksimalni broj nivoa koje skip lista moze imati
	size        int
	prob        float64 // verovatnoca za kreiranje viseg nivoa
	segmentPath string
}

func (s *SkipListMemtable) SetSegmentPath(path string) {
	s.segmentPath = path
}

func (s *SkipListMemtable) GetSegmentPath() string {
	return s.segmentPath
}

func (s *SkipListMemtable) randomLevel() int {
	level := 1
	for rand.Float64() < s.prob && level < s.maxLevel {
		level++
	}
	return level
}

// Konstruktor za skiplistu
func NewSkipListMemtable(maxLevel int, prob float64) *SkipListMemtable {
	return &SkipListMemtable{
		head:     &SkipListNode{next: make([]*SkipListNode, maxLevel)},
		level:    1,
		maxLevel: maxLevel,
		prob:     prob,
		size:     0,
	}
}

func NewSkipListNode(key string, value []byte, tombstone bool, level int) *SkipListNode {
	return &SkipListNode{
		key:       key,
		value:     value,
		tombstone: tombstone,
		next:      make([]*SkipListNode, level), // ili .forward ako si tako nazvao
	}
}

////////////////// VIZUELNO ///////////////////////
/*
neka je ovo pocetno stanje
Level 3: [HEAD]
Level 2: [HEAD]
Level 1: [HEAD]
Level 0: [HEAD]
*/

//////////////////// PUT ('banana', 'zuto')
/*
recimo da u randomLevel() dobijemo vrednost 2, to znaci da u sve liste dodajemo vrednost
Level 2: [HEAD]
Level 1: [HEAD] -> [banana]
Level 0: [HEAD] -> [banana]
*/

//////////////////// PUT ('apple', 'crveno')
/*
randomLevel() nam je dao vrednost 3
Level 2: [HEAD] -> [apple]
Level 1: [HEAD] -> [apple] -> [banana]
Level 0: [HEAD] -> [apple] -> [banana]
apple dolazi pre banana leksikografki pa ide ispred
*/

//////////////////// PUT ('cherry', 'crveno')
/*
randmLevel() - vrednost 1
Level 2: [HEAD] -> [apple]
Level 1: [HEAD] -> [apple] -> [banana]
Level 0: [HEAD] -> [apple] -> [banana] -> [cherry]
*/

//////////////////// GET ('banana')
/*
pocinjes sa najviseg nivoa, head - apple, (key 'apple' < 'banana') ides desno, ali iza apple nema nikog, silazimo level dole
head-apple-banana Pronasli!
*/

//////////////////// DELETE ('apple')
/*
iduci po nivoima od najviseg ka najnizem kada nadjemo 'apple' iskljucimo ga na svim nivoima
*/

func (s *SkipListMemtable) Put(key string, value []byte) {
	update := make([]*SkipListNode, s.maxLevel)
	current := s.head

	// Krecemo od najviseg sloja i silazimo
	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		update[i] = current
	}

	// Sada smo na dnu (nivo 0)
	current = current.next[0]

	// Ako cvor postoji - azuriramo vrednost
	if current != nil && current.key == key {
		current.value = value
		return
	}

	// Inace - kreiramo novi cvor sa random nivoom
	newLevel := s.randomLevel()
	if newLevel > s.level {
		for i := s.level; i < newLevel; i++ {
			update[i] = s.head
		}
		s.level = newLevel
	}

	newNode := &SkipListNode{
		key:   key,
		value: value,
		next:  make([]*SkipListNode, newLevel),
	}

	// Povezemo novi cvor na svim nivoima
	for i := 0; i < newLevel; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	s.size++
}

func (s *SkipListMemtable) Get(key string) ([]byte, bool) {
	current := s.head

	// Krecemo od najvise nivoa i silazimo
	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
	}

	// Sada smo na nivou 0 - sledeci cvor bi mogao biti bas nas
	current = current.next[0]

	/*if current != nil && current.key == key {
		return current.value, true
	}

	return nil, false*/

	if current != nil && current.key == key {
		if current.tombstone {
			return nil, true
		}
		return current.value, true
	}
	return nil, false
}

func (s *SkipListMemtable) Delete(key string) {
	/*update := make([]*SkipListNode, s.maxLevel)
	current := s.head

	// 1. Trazimo sve cvorove koje treba azurirati
	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		update[i] = current
	}

	// 2. Pozicioniramo se na potencijalni cvor za brisanje
	current = current.next[0]

	// 3. Ako postoji brisemo ga na svakom nivou
	if current != nil && current.key == key {
		for i := 0; i < s.level; i++ {
			if update[i].next[i] != current {
				break
			}
			update[i].next[i] = current.next[i]
		}
		s.size--

		// 4. Smanjujemo nivo ako su najvisi slojevi prazni
		for s.level > 1 && s.head.next[s.level-1] == nil {
			s.level--
		}
	}*/

	current := s.head
	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
	}

	current = current.next[0]
	if current != nil && current.key == key {
		current.tombstone = true
	} else {
		// Ako kljuc NE postoji → dodaj novi tombstone čvor
		level := s.randomLevel()
		newNode := &SkipListNode{
			key:       key,
			value:     nil,
			tombstone: true,
			next:      make([]*SkipListNode, level),
		}

		update := make([]*SkipListNode, s.maxLevel)
		current = s.head
		for i := s.level - 1; i >= 0; i-- {
			for current.next[i] != nil && current.next[i].key < key {
				current = current.next[i]
			}
			update[i] = current
		}

		if level > s.level {
			for i := s.level; i < level; i++ {
				update[i] = s.head
			}
			s.level = level
		}

		for i := 0; i < level; i++ {
			newNode.next[i] = update[i].next[i]
			update[i].next[i] = newNode
		}

		s.size++
	}
}

func (s *SkipListMemtable) Size() int {
	return s.size
}

func (s *SkipListMemtable) FlushToSSTable(dirPath string, bm *blockmanager.BlockManager) error {
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("ne mogu da napravim SSTable folder: %v", err)
	}

	var entries []sstable.Entry
	current := s.head.next[0]

	for current != nil {
		entries = append(entries, sstable.Entry{
			Key:       current.key,
			Value:     current.value,
			Tombstone: current.tombstone,
			Timestamp: uint64(time.Now().UnixNano()),
		})
		current = current.next[0]
	}

	return sstable.WriteAllFilesWithBlocks(dirPath, entries, bm)
}

func (s *SkipListMemtable) RangeScan(from, to string) map[string][]byte {
	results := make(map[string][]byte)

	current := s.head.next[0]

	// Idi do prvog kljuca >= from
	for current != nil && current.key < from {
		current = current.next[0]
	}

	// Prikupljaj dokle god smo <= to
	for current != nil && current.key <= to {
		results[current.key] = current.value
		current = current.next[0]
	}

	return results
}
