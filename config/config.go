package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Struktura koja odgovara JSON fajlu
type Config struct {
	MemtableType         string `json:"memtable_type"`
	MemtableMaxEntries   int    `json:"memtable_max_entries"`
	MemtableMaxTables    int    `json:"memtable_max_tables"`
	WALSegmentSize       int    `json:"wal_segment_size"`
	MaxSSTableFiles      int    `json:"max_sstable_files"`
	MaxSSTableLevels     int    `json:"max_levels"`
	SSTableFilesPerLevel int    `json:"sstable_files_per_level"`
	BlockSizeKBK         int    `json:"block_size_kb"`
	CacheCapacity        int    `json:"cache_capacity"`
}

// Globalna promenljiva u koju ucitavamo konfiguraciju
var Current Config

// Funkcija koja cita JSON fajl i popunjava Config strukturu
func LoadConfig(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("greska pri otvaranju konfiguracionog fajla: %v", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&Current)
	if err != nil {
		return fmt.Errorf("greska pri ucitavanju JSON konfiguracije: %v", err)
	}

	fmt.Println("Konfiguracija uspesno ucitanaL:", Current)
	return nil
}
