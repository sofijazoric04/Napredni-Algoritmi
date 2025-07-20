package sstable

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"napredni/blockmanager"
	"napredni/bloomfilter"
	"napredni/config"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// sadrzi binarne fajlove DATA, INDEX, SUMMARY, BLOOM I MERKLE
// data fajl je binarni fajl koji sadrzi sve informacije TIMESTAMP|TOMBSTONE|KEYSIZE|VALUESIZE|KEY|VALUE
// index fajl je dodatni fajl koji se pravi uz data i sadrzi kljuc i offset
// summary fajl je sazetak index fajlq KEYSIZE|KEY|OFFSET

type Entry struct {
	Key       string
	Value     []byte
	Tombstone bool
	Timestamp uint64
}

// pomocne funkcije i strukture neophodne jer se koristi sort.Interface koji mora da ima funkcije LEN, SWAP, LESS u njima definisemo kako sortiramo podatke, u nasem slucaju je sve po kljucu
// pomocna struktura za sortiranje

type byKey []Entry

func (e byKey) Len() int {
	return len(e)
}

func (e byKey) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func (e byKey) Less(i, j int) bool {
	return e[i].Key < e[j].Key
}

// data fajl je jedini fajl sstable-a koji pisemo preko blokova

func WriteDataFileWithBlocks(entries []Entry, path string, bm *blockmanager.BlockManager) error {
	sort.Sort(byKey(entries))

	for blockNum, entry := range entries {
		keyBytes := []byte(entry.Key)
		keySize := uint64(len(keyBytes))
		valueSize := uint64(len(entry.Value))

		tmp := make([]byte, 8)

		buf := make([]byte, 0)

		binary.LittleEndian.PutUint64(tmp, entry.Timestamp)
		buf = append(buf, tmp...)

		if entry.Tombstone {
			buf = append(buf, byte(1))
		} else {
			buf = append(buf, byte(0))
		}

		binary.LittleEndian.PutUint64(tmp, keySize)
		buf = append(buf, tmp...)

		binary.LittleEndian.PutUint64(tmp, valueSize)
		buf = append(buf, tmp...)

		buf = append(buf, keyBytes...)

		buf = append(buf, entry.Value...)

		blockID := blockmanager.BlockID{Path: path, Num: int64(blockNum)}
		err := bm.WriteBlock(blockID, buf)
		if err != nil {
			return err
		}
	}

	return nil
}

// cita sve entrije iz SSTable fajla koriscenjem blokova
func ReadDataFileWithBlocks(path string, numEntries int, bm *blockmanager.BlockManager) ([]Entry, error) {
	var entries []Entry

	for blockNum := 0; blockNum < numEntries; blockNum++ {
		blockID := blockmanager.BlockID{Path: path, Num: int64(blockNum)}
		data, err := bm.ReadBlock(blockID)
		if err != nil {
			return nil, fmt.Errorf("greska pri citanju bloka: %v", err)
		}

		header := data[:25]
		timestamp := binary.LittleEndian.Uint64(header[0:8])
		tombstone := header[8] == 1
		keySize := binary.LittleEndian.Uint64(header[9:17])
		valueSize := binary.LittleEndian.Uint64(header[17:25])

		// vaALIDACIJA - pre nego što pokušamo da napravimo slice
		if 25+keySize+valueSize > uint64(len(data)) {
			return nil, fmt.Errorf("korumpiran blok: keySize=%d, valueSize=%d, ukupno treba %d bajta, ali blok ima samo %d bajta", keySize, valueSize, 25+keySize+valueSize, len(data))
		}

		key := data[25 : 25+keySize]
		value := data[25+keySize : 25+keySize+valueSize]

		entry := Entry{
			Key:       string(key),
			Value:     value,
			Tombstone: tombstone,
			Timestamp: timestamp,
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// svaki zapis idu u svoj blok: keysize|key|offset
func WriteIndexFileWithBlocks(entries []Entry, indexPath string, dataOffset []int64, bm *blockmanager.BlockManager) error {
	tmp := make([]byte, 8)
	blockNum := int64(0) // broj bloka krece od 0

	for i, entry := range entries {
		keyBytes := []byte(entry.Key)
		keySize := uint64(len(keyBytes))

		buf := make([]byte, 0)

		binary.LittleEndian.PutUint64(tmp, keySize)
		buf = append(buf, tmp...)

		buf = append(buf, keyBytes...)

		binary.LittleEndian.PutUint64(tmp, uint64(dataOffset[i]))
		buf = append(buf, tmp...)

		blockID := blockmanager.BlockID{Path: indexPath, Num: blockNum}
		err := bm.WriteBlock(blockID, buf)
		if err != nil {
			return err
		}
		blockNum++
	}

	return nil
}

// cita blok po blok
func FindKeyInIndexWithBlocks(bm *blockmanager.BlockManager, indexPath string, targetKey string) (int64, bool) {
	blockNum := int64(0)

	for {
		blockID := blockmanager.BlockID{Path: indexPath, Num: blockNum}
		data, err := bm.ReadBlock(blockID)
		if err != nil {
			break
		}

		tmp := data[:8]
		keySize := binary.LittleEndian.Uint64(tmp)

		keyBytes := data[8 : 8+keySize]
		key := string(keyBytes)

		offsetBytes := data[8+keySize : 8+keySize+8]
		offset := int64(binary.LittleEndian.Uint64(offsetBytes))

		if key == targetKey {
			return offset, true
		}

		blockNum++

	}
	return -1, false
}

func ReadDataEntryAtOffset(file *os.File, offset int64) (Entry, error) {
	_, err := file.Seek(offset, io.SeekCurrent)
	if err != nil {
		return Entry{}, fmt.Errorf("ne mogu da se pozicioniram u fajlu: %v", err)
	}

	header := make([]byte, 8+1+8+8)
	_, err = file.Read(header)
	if err != nil {
		return Entry{}, fmt.Errorf("greska pri citanju zagljavlja: %v", err)
	}

	timestamp := binary.LittleEndian.Uint64(header[0:8])
	tombstone := header[8] == 1
	keySize := binary.LittleEndian.Uint64(header[9:17])
	valueSize := binary.LittleEndian.Uint64(header[17:25])

	key := make([]byte, keySize)
	_, err = file.Read(key)
	if err != nil {
		return Entry{}, fmt.Errorf("greska pri citanju kljuca: %v", err)
	}

	value := make([]byte, valueSize)
	_, err = file.Read(value)
	if err != nil {
		return Entry{}, fmt.Errorf("greska pri citanju vrendosti : %v", err)
	}

	return Entry{
		Key:       string(key),
		Value:     value,
		Tombstone: tombstone,
		Timestamp: timestamp,
	}, nil

}

func FastGetFromSSTablesWithBlocks(baseDir string, targetKey string, bm *blockmanager.BlockManager) ([]byte, bool) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		fmt.Println("greska pri citanju direktorijuma:", err)
		return nil, false
	}

	var sstableDirs []string
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "sstable_") {
			sstableDirs = append(sstableDirs, entry.Name())
		}
	}

	sort.Sort(sort.Reverse(sort.StringSlice(sstableDirs)))

	for _, dir := range sstableDirs {
		sstablePath := filepath.Join(baseDir, dir)
		indexPath := filepath.Join(sstablePath, "index")
		dataPath := filepath.Join(sstablePath, "data")
		summaryPath := filepath.Join(sstablePath, "summary")

		var startOffset int64 = 0
		if _, err := os.Stat(summaryPath); err == nil {
			offset, found := FindClosestIndexOffsetWithBlocks(bm, summaryPath, targetKey)
			if found {
				startOffset = offset
			}
		}

		offset, found := FindKeyInIndexFromWithBlocks(bm, indexPath, targetKey, startOffset)
		if !found {
			continue
		}

		entry, err := ReadDataEntryAtBlock(bm, dataPath, offset)
		if err != nil {
			fmt.Println("greska pri citanju zapisa:", err)
			continue
		}

		if entry.Tombstone {
			return nil, false
		}

		return entry.Value, true
	}

	return nil, false
}

func FindKeyInIndexFrom(indexPath string, targetKey string, startOffset int64) (int64, bool) {
	file, err := os.Open(indexPath)
	if err != nil {
		fmt.Println("Greška pri otvaranju index fajla:", err)
		return -1, false
	}
	defer file.Close()

	_, err = file.Seek(startOffset, io.SeekStart)
	if err != nil {
		fmt.Println("Greška pri seek operaciji:", err)
		return -1, false
	}

	tmp := make([]byte, 8)

	for {
		_, err := file.Read(tmp)
		if err != nil {
			break
		}
		keySize := binary.LittleEndian.Uint64(tmp)

		keyBytes := make([]byte, keySize)
		_, err = file.Read(keyBytes)
		if err != nil {
			break
		}
		key := string(keyBytes)

		_, err = file.Read(tmp)
		if err != nil {
			break
		}
		dataOffset := int64(binary.LittleEndian.Uint64(tmp))

		if key == targetKey {
			return dataOffset, true
		}
	}

	return -1, false
}

func FindKeyInIndexFromWithBlocks(bm *blockmanager.BlockManager, indexPath string, targetKey string, startBlockNum int64) (int64, bool) {
	blockNum := startBlockNum

	for {
		blockID := blockmanager.BlockID{Path: indexPath, Num: blockNum}
		data, err := bm.ReadBlock(blockID)
		if err != nil {
			break
		}

		tmp := data[:8]
		keySize := binary.LittleEndian.Uint64(tmp)

		keyBytes := data[8 : 8+keySize]
		key := string(keyBytes)

		offsetBytes := data[8+keySize : 8+keySize+8]
		offset := int64(binary.LittleEndian.Uint64(offsetBytes))

		if key == targetKey {
			return offset, true
		}
		blockNum++

	}

	return -1, false

}

func WriteSummaryFile(indexPath string, summaryPath string, samplingRate int) error {
	indexFile, err := os.Open(indexPath)
	if err != nil {
		return fmt.Errorf("ne mogu da otvorim index fajl: %v", err)
	}
	defer indexFile.Close()

	summaryFile, err := os.Create(summaryPath)
	if err != nil {
		return fmt.Errorf("ne mogu da kreiram summary fajl: %v", err)
	}
	defer summaryFile.Close()

	tmp := make([]byte, 8)

	var indexOffset int64 = 0
	var counter = 0
	for {
		_, err := indexFile.Read(tmp)
		if err != nil {
			break
		}
		keySize := binary.LittleEndian.Uint64(tmp)

		key := make([]byte, keySize)
		_, err = indexFile.Read(key)
		if err != nil {
			break
		}

		_, err = indexFile.Read(tmp)
		if err != nil {
			break
		}

		_ = binary.LittleEndian.Uint64(tmp)

		// ako je redni broj deljiv sa samplingRate - upisujemo u summary
		if counter%samplingRate == 0 {
			binary.LittleEndian.PutUint64(tmp, keySize)
			summaryFile.Write(tmp)

			summaryFile.Write(key)

			binary.LittleEndian.PutUint64(tmp, uint64(indexOffset))
			summaryFile.Write(tmp)
		}

		indexOffset += 8 + int64(keySize) + 8
		counter++
	}

	return nil
}

func WriteSummaryFileWithBlocks(indexPath string, summaryPath string, samplingRate int, bm *blockmanager.BlockManager) error {
	tmp := make([]byte, 8)
	blockNum := int64(0)
	summaryBlockNum := int64(0)
	counter := 0

	for {
		blockID := blockmanager.BlockID{Path: indexPath, Num: blockNum}
		data, err := bm.ReadBlock(blockID)
		if err != nil {
			break
		}

		if len(data) < 16 {
			blockNum++
			continue
		}

		keySize := binary.LittleEndian.Uint64(data[:8])

		if keySize > uint64(len(data)-16) {
			fmt.Printf(" Nevalidan keySize: %d u bloku %d (preskačem)\n", keySize, blockNum)
			blockNum++
			continue
		}

		keyBytes := data[8 : 8+keySize]

		indexOffset := blockNum * int64(bm.BlockSize())

		if counter%samplingRate == 0 {
			buf := make([]byte, 0)

			binary.LittleEndian.PutUint64(tmp, keySize)
			buf = append(buf, tmp...)

			buf = append(buf, keyBytes...)

			binary.LittleEndian.PutUint64(tmp, uint64(indexOffset))
			buf = append(buf, tmp...)

			summaryBlockID := blockmanager.BlockID{Path: summaryPath, Num: summaryBlockNum}
			err := bm.WriteBlock(summaryBlockID, buf)
			if err != nil {
				return err
			}
			summaryBlockNum++
		}
		counter++
		blockNum++
	}
	return nil
}

func FindClosestIndexOffset(summaryPath, targetKey string) (int64, bool) {
	file, err := os.Open(summaryPath)
	if err != nil {
		fmt.Println("ne mogu da otvorim summary fajl:", err)
		return -1, false
	}
	defer file.Close()

	var bestMatchOffset int64 = -1
	var tmp = make([]byte, 8)

	for {
		_, err := file.Read(tmp)
		if err != nil {
			break
		}
		keySize := binary.LittleEndian.Uint64(tmp)

		keyBytes := make([]byte, keySize)
		_, err = file.Read(keyBytes)
		if err != nil {
			break
		}
		key := string(keyBytes)

		_, err = file.Read(tmp)
		if err != nil {
			break
		}
		offset := int64(binary.LittleEndian.Uint64(tmp))

		if key <= targetKey {
			bestMatchOffset = offset
		} else {
			break
		}
	}

	if bestMatchOffset == -1 {
		return -1, false
	}

	return bestMatchOffset, true

}

func FindClosestIndexOffsetWithBlocks(bm *blockmanager.BlockManager, summaryPath string, targetKey string) (int64, bool) {
	blockNum := int64(0)
	var bestMatchOffset int64 = -1

	for {
		blockID := blockmanager.BlockID{Path: summaryPath, Num: blockNum}
		data, err := bm.ReadBlock(blockID)
		if err != nil {
			break
		}

		tmp := data[:8]
		keySize := binary.LittleEndian.Uint64(tmp)

		keyBytes := data[8 : 8+keySize]
		key := string(keyBytes)

		offsetBytes := data[8+keySize : 8+keySize+8]
		offset := int64(binary.LittleEndian.Uint64(offsetBytes))

		if key <= targetKey {
			bestMatchOffset = offset
		} else {
			break
		}

		blockNum++
	}

	if bestMatchOffset == -1 {
		return -1, false
	}

	return bestMatchOffset, true
}

func CompactSSTables(sstableDir string, bm *blockmanager.BlockManager) error {
	files, err := os.ReadDir(sstableDir)
	if err != nil {
		return fmt.Errorf("ne mogu da procitam direktorijum SSTable: %v", err)
	}

	sstableFolders := []string{}
	for _, f := range files {
		if f.IsDir() && strings.HasPrefix(f.Name(), "sstable_") {
			sstableFolders = append(sstableFolders, filepath.Join(sstableDir, f.Name()))
		}
	}

	if len(sstableFolders) < 2 {
		fmt.Println("nema potrebe za kompaktiranjem, postoji manje od 2 sstable-a")
		return nil
	}

	merged := make(map[string]Entry)

	for _, folder := range sstableFolders {
		metaPath := filepath.Join(folder, "meta")
		dataPath := filepath.Join(folder, "data")

		count, err := LoadMeta(metaPath)
		if err != nil {
			return fmt.Errorf("greska pri citanju meta fajla: %v", err)
		}

		entries, err := ReadDataFileWithBlocks(dataPath, int(count), bm)
		if err != nil {
			return fmt.Errorf("greska pri citanju SSTable %s: %v", folder, err)
		}

		for _, e := range entries {
			if existing, ok := merged[e.Key]; !ok || e.Timestamp > existing.Timestamp {
				merged[e.Key] = e
			}
		}
	}

	var finalEntries []Entry
	for _, entry := range merged {
		if !entry.Tombstone {
			finalEntries = append(finalEntries, entry)
		}
	}
	sort.Slice(finalEntries, func(i, j int) bool {
		return finalEntries[i].Key < finalEntries[j].Key
	})

	timestamp := time.Now().UnixNano()
	newDir := filepath.Join(sstableDir, fmt.Sprintf("sstable_L0_%d", timestamp))
	err = os.MkdirAll(newDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("ne mogu da napravim novi SSTable folder: %v", err)
	}

	err = WriteAllFilesWithBlocks(newDir, finalEntries, bm)
	if err != nil {
		return fmt.Errorf("ne mogu da upisem novi SSTable: %v", err)
	}

	for _, old := range sstableFolders {
		if err := os.RemoveAll(old); err != nil {
			fmt.Printf(" Ne mogu da obrišem %s: %v\n", old, err)
		}
	}

	fmt.Printf(" Kompaktiranje uspešno! Napravljen novi SSTable (%d zapisa).\n", len(finalEntries))
	return nil
}

func ReadDataEntryAtBlock(bm *blockmanager.BlockManager, path string, blockNum int64) (Entry, error) {
	blockID := blockmanager.BlockID{Path: path, Num: blockNum}
	data, err := bm.ReadBlock(blockID)
	if err != nil {
		return Entry{}, fmt.Errorf("ne mogu da procitam blok: %v", err)
	}

	header := data[:25]
	timestamp := binary.LittleEndian.Uint64(header[0:8])
	tombstone := header[8] == 1
	keySize := binary.LittleEndian.Uint64(header[9:17])
	valueSize := binary.LittleEndian.Uint64(header[17:25])

	key := data[25 : 25+keySize]
	value := data[25+keySize : 25+keySize+valueSize]

	return Entry{
		Key:       string(key),
		Value:     value,
		Tombstone: tombstone,
		Timestamp: timestamp,
	}, nil
}

func WriteAllFilesWithBlocks(dirPath string, entries []Entry, bm *blockmanager.BlockManager) error {
	dataPath := filepath.Join(dirPath, "data")
	err := WriteDataFileWithBlocks(entries, dataPath, bm)
	if err != nil {
		return fmt.Errorf("ne mogu da upisem data fajl: %v", err)
	}

	metaPath := filepath.Join(dirPath, "meta")
	metaFile, err := os.Create(metaPath)
	if err != nil {
		return fmt.Errorf("ne mogu da kreiram meta fajl: %v", err)
	}
	defer metaFile.Close()
	err = binary.Write(metaFile, binary.LittleEndian, int64(len(entries)))
	if err != nil {
		return fmt.Errorf("ne mogu da upisem broj zapisa u meta fajl: %v", err)
	}

	indexPath := filepath.Join(dirPath, "index")
	var offsets []int64
	for i := range entries {
		offsets = append(offsets, int64(i))
	}
	err = WriteIndexFileWithBlocks(entries, indexPath, offsets, bm)
	if err != nil {
		return fmt.Errorf("ne mogu da upisem index fajl: %v", err)
	}

	summaryPath := filepath.Join(dirPath, "summary")
	err = WriteSummaryFileWithBlocks(indexPath, summaryPath, 10, bm)
	if err != nil {
		return fmt.Errorf("ne mogu da upisem summary fajl: %v", err)
	}

	bf := bloomfilter.NewBloomFilter(len(entries), 0.01)
	for _, entry := range entries {
		if !entry.Tombstone {
			bf.Add(entry.Key)
		}
	}
	bloomPath := filepath.Join(dirPath, "bloom")
	err = bf.SaveToFile(bloomPath)
	if err != nil {
		return fmt.Errorf("ne mogu da upisem bloom filter: %v", err)
	}

	root := GenerateMerkleRoot(entries)
	merklePath := filepath.Join(dirPath, "merkle")
	err = os.WriteFile(merklePath, []byte(hex.EncodeToString(root)), 0644)
	if err != nil {
		return fmt.Errorf("ne mogu da upisem merkle root: %v", err)
	}

	return nil
}

// izvlaci nivo ss tabla
func ExtractLevelFromFolder(folderName string) int {
	parts := strings.Split(folderName, "_")
	if len(parts) < 2 {
		return 0
	}
	levelStr := strings.TrimPrefix(parts[1], "L")
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		return 0
	}
	return level
}

func CompactLevel(sstableDir string, level int, bm *blockmanager.BlockManager) error {
	entries, err := os.ReadDir(sstableDir)
	if err != nil {
		return fmt.Errorf("ne mogu da procitam sstable direktorijum: %v", err)
	}

	var foldersOnLevel []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		folderName := entry.Name()
		if !strings.HasPrefix(folderName, "sstable_L") {
			continue
		}
		folderLevel := ExtractLevelFromFolder(folderName)
		if folderLevel == level {
			foldersOnLevel = append(foldersOnLevel, folderName)
		}
	}

	if len(foldersOnLevel) <= config.Current.SSTableFilesPerLevel {
		return nil
	}

	fmt.Printf(" Pokrećem kompakciju za nivo %d...\n", level)

	var allEntries []Entry
	for _, folderName := range foldersOnLevel {
		folderPath := filepath.Join(sstableDir, folderName)
		metaPath := filepath.Join(folderPath, "meta")
		dataPath := filepath.Join(folderPath, "data")

		count, err := LoadMeta(metaPath)
		if err != nil {
			return fmt.Errorf("greska pri citanju meta fajla: %v", err)
		}

		entries, err := ReadDataFileWithBlocks(dataPath, int(count), bm)
		if err != nil {
			return fmt.Errorf("greska pri citanju SSTable foldera %s: %v", folderName, err)
		}
		allEntries = append(allEntries, entries...)
	}

	sort.Slice(allEntries, func(i, j int) bool {
		return allEntries[i].Key < allEntries[j].Key
	})

	newLevel := level + 1
	newFolderName := fmt.Sprintf("sstable_L%d_%d", newLevel, time.Now().UnixNano())
	newFolderPath := filepath.Join(sstableDir, newFolderName)

	err = os.MkdirAll(newFolderPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("ne mogu da napravim novi SSTable folder: %v", err)
	}

	err = WriteAllFilesWithBlocks(newFolderPath, allEntries, bm)
	if err != nil {
		return fmt.Errorf("ne mogu da upisem fajlove: %v", err)
	}

	for _, folderName := range foldersOnLevel {
		fullPath := filepath.Join(sstableDir, folderName)
		err := os.RemoveAll(fullPath)
		if err != nil {
			fmt.Printf(" Ne mogu da obrišem %s: %v\n", fullPath, err)
		}
	}

	fmt.Printf("Kompaktiranje nivoa %d uspešno! Novi nivo %d.\n", level, newLevel)
	return nil
}

// funkcija koja iterira kroz nivoe
func AutoCompact(sstableDir string, bm *blockmanager.BlockManager) error {
	maxLevels := config.Current.MaxSSTableLevels

	for level := 0; level < maxLevels; level++ {
		err := CompactLevel(sstableDir, level, bm)
		if err != nil {
			return fmt.Errorf("greska pri kompaktiranju nivoa %d: %v", level, err)
		}
	}
	return nil
}

func LoadMeta(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("ne mogu da procitam meta fajl: %v", err)
	}
	return int64(binary.LittleEndian.Uint64(data)), nil
}

func ValidateMerkleTree(dirPath string, bm *blockmanager.BlockManager) (bool, error) {
	dataPath := filepath.Join(dirPath, "data")
	metaPath := filepath.Join(dirPath, "meta")
	merklePath := filepath.Join(dirPath, "merkle")

	count, err := LoadMeta(metaPath)
	if err != nil {
		return false, fmt.Errorf("ne mogu da ucitam meta fajl: %v", err)
	}

	entries, err := ReadDataFileWithBlocks(dataPath, int(count), bm)
	if err != nil {
		return false, fmt.Errorf("ne mogu da ucitam data fajl: %v", err)
	}

	currentRoot := GenerateMerkleRoot(entries)

	savedRoot, err := LoadMerkleRoot(merklePath)
	if err != nil {
		return false, fmt.Errorf("ne mogu da ucitam merkle fajl: %v", err)
	}

	if hex.EncodeToString(currentRoot) == hex.EncodeToString(savedRoot) {
		return true, nil
	}

	return false, nil
}
