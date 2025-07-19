package memtable

type SnapshotEntry struct {
	Key       string
	Value     []byte
	Tombstone bool
}
