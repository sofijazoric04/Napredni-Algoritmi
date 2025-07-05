package cache

import "sync"

// Node predstavlja jedan cvor u LRU listi
type Node struct {
	key   string
	value []byte
	prev  *Node
	next  *Node
}

// LRU cache - ceo kes
type LRUCache struct {
	capacity int              // maksimalan broj elemenata u kesu
	items    map[string]*Node // mapa za brz pristup cvorovima
	head     *Node            // najskorije koriscen
	tail     *Node            // najmanje koriscen
	lock     sync.Mutex
}

// NewLRUCache pravi novi kes sa datim kapacitetima
func NewLRUCache(cap int) *LRUCache {
	return &LRUCache{
		capacity: cap,
		items:    make(map[string]*Node),
	}
}

// Get pokusava da pronadje vrednosot za dati kljuc
// Ako postoji - vraca vrednost i true
// Ako ne postoji - vraca nil i false
func (lru *LRUCache) Get(key string) ([]byte, bool) {
	node, exists := lru.items[key]
	if !exists {
		return nil, false
	}

	// Pomeri cvor na pocetak liste
	lru.moveToFront(node)

	return node.value, true
}

func (lru *LRUCache) moveToFront(node *Node) {
	if node == lru.head {
		return // vec je na vrhu
	}

	// Ukloni ga iz trenutne pozicije
	lru.removeNode(node)

	// Postavi kao novi head
	node.prev = nil
	node.next = lru.head

	if lru.head != nil {
		lru.head.prev = node
	}
	lru.head = node

	// Ako lista nema tail, postavi ga
	if lru.tail == nil {
		lru.tail = node
	}
}

// Uklanja cvor iz povezane liste, ali ne brise iz mape
func (lru *LRUCache) removeNode(node *Node) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		lru.head = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		lru.tail = node.prev
	}
}

// Put dodaje novi kljuc i vrednost u kes
// Ako je kes pun, uklanja se najmanje koriscen (tail)
func (lru *LRUCache) Put(key string, value []byte) {
	// Ako vec postoji - samo azuriraj vrednost i premesti na pocetak
	if node, exists := lru.items[key]; exists {
		node.value = value
		lru.moveToFront(node)
		return
	}

	// Kreiramo nov cvor
	newNode := &Node{
		key:   key,
		value: value,
	}

	// Dodajemo ga na pocetak liste
	newNode.next = lru.head
	if lru.head != nil {
		lru.head.prev = newNode
	}
	lru.head = newNode

	// Ako je lista bila prazna, setuj i tail
	if lru.tail == nil {
		lru.tail = newNode
	}

	// Dodaj u mapu
	lru.items[key] = newNode

	// Ako smo premasili kapacitet - izbacujemo tail
	if len(lru.items) > lru.capacity {
		// Sacuvamo tail pre uklanjajna
		evicted := lru.tail

		// Uklonimo iz liste
		lru.removeNode(evicted)

		// Obrisemo ga iz mape
		delete(lru.items, evicted.key)

	}
}

// Remove uklanja kljuc iz kesa ako postoji
func (lru *LRUCache) Remove(key string) {
	node, exists := lru.items[key]
	if !exists {
		return // nema šta da brišemo
	}

	// Uklanjamo čvor iz povezane liste
	lru.removeNode(node)

	// Brišemo iz mape
	delete(lru.items, key)
}

// Items vraca mapu svih elemenata u kesu
func (lru *LRUCache) Items() map[string][]byte {
	lru.lock.Lock()
	defer lru.lock.Unlock()

	result := make(map[string][]byte)
	for k, e := range lru.items {
		result[k] = e.value
	}
	return result
}
