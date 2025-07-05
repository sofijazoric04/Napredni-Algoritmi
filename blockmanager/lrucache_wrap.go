package blockmanager

import (
	"fmt"
	"kvstore/cache"
)

// BlockCache je adapter LRU
type BlockCache struct {
	lru *cache.LRUCache
}

func NewBlockCache(capacity int) *BlockCache {
	return &BlockCache{
		lru: cache.NewLRUCache(capacity),
	}
}

func (bc *BlockCache) keyString(id BlockID) string {
	return fmt.Sprintf("%s:%d", id.Path, id.Num) // path:num
}

func (bc *BlockCache) Get(id BlockID) ([]byte, bool) {
	return bc.lru.Get(bc.keyString(id))
}

func (bc *BlockCache) Put(id BlockID, data []byte) {
	bc.lru.Put(bc.keyString(id), data)
}

func (bc *BlockCache) Remove(id BlockID) {
	bc.lru.Remove(bc.keyString(id))
}
