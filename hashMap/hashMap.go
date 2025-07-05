package main

import "fmt"

type HashMap struct {
	key   string
	value string
}

type Bucket struct {
	podaciLista []HashMap // buckwt contains key-value pairs hashed to the same index
}

type HashTable struct {
	buckets []*Bucket //this is a slice of Bucket. This is a list of buckets
	size    int       //size of the table (number of buckets)
}

func NewHashTable(capacity int) *HashTable {
	buckets := make([]*Bucket, capacity)
	for i := range buckets {
		buckets[i] = &Bucket{podaciLista: []HashMap{}} //initialize each bucket with an empty list
	}
	return &HashTable{buckets: buckets}
}

func (ht *HashTable) hashFunction(key string) int {
	hash := 0
	for _, char := range key {
		hash = (hash*31 + int(char)) % len(ht.buckets)
	}
	return hash
}

func (ht *HashTable) Put(key string, value string) {
	index := ht.hashFunction(key)
	bucket := ht.buckets[index]
	for i, kv := range bucket.podaciLista {
		if kv.key == key {
			bucket.podaciLista[i].value = value //updating the value if the key already exists
			return
		}
	}
	bucket.podaciLista = append(bucket.podaciLista, HashMap{key: key, value: value}) //adding a new key-value pair
	ht.size++

	if float64(ht.size)/float64(len(ht.buckets)) > 0.75 { //if the load factor is greater than 0.75, resize the table
		ht.resize()
	}
}

func (ht *HashTable) Get(key string) (string, bool) {
	index := ht.hashFunction(key)
	bucket := ht.buckets[index]
	for _, kv := range bucket.podaciLista {
		if kv.key == key {
			return kv.value, true
		}
	}
	return "", false
}

func (ht *HashTable) Delete(key string) bool {
	index := ht.hashFunction(key)
	bucket := ht.buckets[index]
	for i, kv := range bucket.podaciLista {
		if kv.key == key {
			bucket.podaciLista[i] = bucket.podaciLista[len(bucket.podaciLista)-1] //replace the key-value pair with the last element
			bucket.podaciLista = bucket.podaciLista[:len(bucket.podaciLista)-1]   //remove the last element
			ht.size--
			return true
		}
	}
	return false
}

func (ht *HashTable) resize() {
	newCapacity := len(ht.buckets) * 2
	newBuckets := make([]*Bucket, newCapacity)
	for i := range newBuckets {
		newBuckets[i] = &Bucket{podaciLista: []HashMap{}}
	}
	//rehash all the key-value pairs
	for _, bucket := range ht.buckets {
		for _, kv := range bucket.podaciLista {
			index := ht.hashFunction(kv.key) % len(newBuckets)
			newBuckets[index].podaciLista = append(newBuckets[index].podaciLista, kv)
		}
	}
	ht.buckets = newBuckets
}

func (ht *HashTable) Size() int {
	return ht.size
}

func (ht *HashTable) showHashMapTable() {
	for i, bucket := range ht.buckets {
		fmt.Printf("Bucket %d\n", i)
		for _, kv := range bucket.podaciLista {
			fmt.Printf("%s: %s\n", kv.key, kv.value)
		}
		fmt.Println()
	}
}

func main() {
	ht := NewHashTable(10)
	ht.Put("key1", "value1")
	ht.Put("key2", "value2")
	ht.Put("key3", "value3")
	ht.Put("key4", "value4")

	fmt.Println(ht.Get("key1"))
	fmt.Println(ht.Get("key3"))

	ht.Delete("key2")

	fmt.Println(ht.Get("key2"))

	ht.Put("key3", "value5")
	fmt.Println(ht.Get("key3"))

	ht.showHashMapTable()
}
