package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"napredni/blockmanager"
	"napredni/memtable"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// U sustini WAL je obican fajl u kojem su informacije BINARNOG formata!

// WAL zapis
// CRC|TIMESTAMP|TOMBSTONE|KEYSIZE|VALUESIZE|KEY|VALUE
// crc se svaki put racuna zato nije deo struct-a
type Record struct {
	Timestamp uint64
	Tombstone bool
	Key       []byte
	Value     []byte
}

// Writer struktura za segmentaciju
// Preko Writer-a mi pozivamo funkcije za zpis i ucitavanje Record-a
type Writer struct {
	dirPath     string // folder u kojem cuvamo fajlove
	segmentSize int    // maksimalan broj zapisa po segmentu
	//currentFile   *os.File // trenutni otvoren fajl
	currentIndex       int // broj trenutnog segmenta
	recordsInFile      int // koliko zapisa ima u trenutnom fajlu
	blockManager       *blockmanager.BlockManager
	currentSegmentPath string
}

// Put zapis u fajl
func WriteRecord(bm *blockmanager.BlockManager, segmentPath string, blockNum int64, record Record) error {
	// Priprema svih delova za binarno upisivanje
	// Od []byte za Key i Value, mi dobijamo njihovu duzinu
	keySize := uint64(len(record.Key))
	valueSize := uint64(len(record.Value))

	// Kreiramo byte slice gde cemo sve podatke da upisemo pre nego ih pisemo u fajl
	buf := make([]byte, 0)

	// Encode sve podatke u binarni oblik
	// Preko tmp cemo da podatke iz njihovih tipova bilo int, bool i slicno da pretvorimo u niz bajtova
	tmp := make([]byte, 8) // privremeni buffer za svako polje // odmah inicijalizovan na duzinu 8 jer prva informacija koju upisujemo je Timestamp(8 bajtova)

	// Timestamp
	binary.LittleEndian.PutUint64(tmp, record.Timestamp) // binary.LittleEndian.PutTIP(tmp, Timestamp) - znaci da mi podatak Timestamp iz uint64 pretvaramo u niz bajtova i dodajemo u nas slice tmp
	// LittleEndian - Little Endian je nacin kako se bajtovi rasporedjuju u memoriji. Kada je vrednost tipa uint64 (koja se sastoji od 8 bajtova), u Little Endian formatu najniži bajt (najmanje značajan) dolazi prvi, a najviši bajt poslednji.
	buf = append(buf, tmp...) // ... unpacking ili sirenje slice-a, sirimo buf slice, tako sto dodajemo pojedinacno el iz tmp slice, da nema ... bilo bi da el iz tmp ubacujemo u buf kao jedan veliki el

	// Tombstone (bool kao 1 bajt)
	if record.Tombstone { // provera da li record koji upisujemo ima polje Tombstone na true, tj da li je taj record logicki obrisan
		buf = append(buf, byte(1))
	} else {
		buf = append(buf, byte(0))
	}

	// KeySize
	binary.LittleEndian.PutUint64(tmp, keySize) // isto sve za keySize, na pocetku je uzeta duzina []byte kljuca, dobio se int, koji sada preko binary.LittleEndian mi pretvaramo u niz bajtova zapisujemo u tmp
	buf = append(buf, tmp...)                   // a onda iz tmp, ga zapisujemo u buf opet preko unpacking...

	// ValueSize
	binary.LittleEndian.PutUint64(tmp, valueSize) // sve isto vazi i za duzinu vrednosti, valueSize
	buf = append(buf, tmp...)

	// Key
	buf = append(buf, record.Key...) // kod kljuca i vrednosti nema potrebe pozivati binary.LE... jer se kljuc i vrednost vec nizovi bajtova []byte, u nasoj Record struct, tako da tu nema konverzije, odmah se zapisuje u buf

	// Value
	buf = append(buf, record.Value...) // isto kao kod kljuca

	// Racunanje CRC32 (kontrola gresaka)
	crc := crc32.ChecksumIEEE(buf)

	// Na pocetak dodajemo CRC(4 bajta)
	// Napravili smo full slice, i full ce upravo biti citav slice podataka koji zapisujemo u fajl, tj u blok, ali o tome kasnije...
	// Najpre se zapisuje CRC pa tek onda ostalo
	full := make([]byte, 4)
	binary.LittleEndian.PutUint32(full, crc)
	full = append(full, buf...)

	// Kreira se novi block, koji se onda zapisuje
	blockID := blockmanager.BlockID{Path: segmentPath, Num: blockNum}
	return bm.WriteBlock(blockID, full)

}

// Funkcija cita sve Record-e
func ReadAllRecords(bm *blockmanager.BlockManager, segmentPath string) ([]Record, error) {
	var records []Record // vracamo niz Record struct-ova
	i := 0
	for {
		fmt.Printf(" Čitam blok broj: %d iz fajla: %s\n", i, segmentPath)

		/* vizuelna slika jednog bloka, dakle on je 4kb, podaci koje upisujemo mozda ali i vrv nece biti tacno 4kb pa ostatak bloka popunjavamo 0
		[0   - 3]      : CRC
		[4   - 11]     : Timestamp
		[12  - 12]     : Tombstone
		[13  - 20]     : Key Size
		[21  - 28]     : Value Size
		[29  - 29+keySize-1]: Key
		[29+keySize - 29+keySize+valueSize-1]: Value
		[ostatak bloka]: NULE, 00 00 00 00 ...
		*/

		// kreira se novi block, koji se cita
		blockID := blockmanager.BlockID{Path: segmentPath, Num: int64(i)}
		// data [valid data][valid data][valid data]...[baj baj baj...][00 00 00 00 00....]
		data, err := bm.ReadBlock(blockID)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("greska pri citanju bloka: %v", err)
		}

		// PROVERA: Ako je blok prazan — znači kraj validnih podataka
		isEmpty := true
		for _, b := range data {
			if b != 0 {
				isEmpty = false
				break
			}
		}
		if isEmpty {
			i++
			continue
		}

		if len(data) < 29 { // zbog cega < 29 jer je CRC 4abjta, Timestamp 8 bajta, Tombston1 bajt, KeySize i ValueSize po 8 = 29, a kljuc i vr onda jos vise
			return nil, fmt.Errorf("blok je premali da sadrži validan zapis")
		}

		expectedCRC := binary.LittleEndian.Uint32(data[0:4]) // sad umesto binary.LE.PUTTIP, nema to PUT neko samo TIP ovo je obrnuto nego kod pisanja, mi sada citamo niz bajtova, ali ih pretvaramo u odredjeni tip, bilo int, bool, ili nesto drugo i stavljamo u promenljivu
		header := data[4 : 4+25]                             // pravimo header koji zapravo sadrzi informacije od 4 bajta do 29 a to su nam Timestamp|Tombstone|KeySize|ValueSize|

		// isto kao gore za CRC preko binary.LE.TIP mi iz niza bajtova dobijamo tip podatka, i cuvamo u promenljivu
		timestamp := binary.LittleEndian.Uint64(header[0:8])
		tombstone := header[8] == 1
		keySize := binary.LittleEndian.Uint64(header[9:17])
		valueSize := binary.LittleEndian.Uint64(header[17:25])

		expectedTotal := 4 + 25 + keySize + valueSize
		if uint64(len(data)) < expectedTotal {
			return nil, fmt.Errorf("blok ne sadrži dovoljno podataka za key+value")
		}

		// citamo kljuc od 29 bajta pa do 29+velicina kljuca
		key := data[29 : 29+keySize]
		// isto za data
		value := data[29+keySize : 29+keySize+valueSize]

		// Rekonstruisanje crcData
		crcData := make([]byte, 0, 8+1+8+8+keySize+valueSize)
		tmp := make([]byte, 8)

		// ovde sada radimo isto kao sto smo i za Write, zbog cega?
		// vracamo sve u binary podatke tj []byte, da bismo za taj Record mogli izracunati CRC
		// ako se ocekivani CRC razlikuje od izracunatog to znaci da je podatak ili ostecen iz nekog razloga ili promenjen

		binary.LittleEndian.PutUint64(tmp, timestamp)
		crcData = append(crcData, tmp...)

		if tombstone {
			crcData = append(crcData, byte(1))
		} else {
			crcData = append(crcData, byte(0))
		}

		binary.LittleEndian.PutUint64(tmp, keySize)
		crcData = append(crcData, tmp...)

		binary.LittleEndian.PutUint64(tmp, valueSize)
		crcData = append(crcData, tmp...)

		crcData = append(crcData, key...)
		crcData = append(crcData, value...)

		calculatedCRC := crc32.ChecksumIEEE(crcData)

		if calculatedCRC != expectedCRC {
			return nil, fmt.Errorf("CRC ne odgovara - podatak mozda ostecen")
		}

		// niz Record-a koji vracamo samo popunimo jednim Record-om i i++ idemo dalje
		records = append(records, Record{
			Timestamp: timestamp,
			Tombstone: tombstone,
			Key:       key,
			Value:     value,
		})

		i++ // sledeći blok
	}

	return records, nil
}

// Konstruktor za Writer
func NewWriter(dirPath string, segmentSize int, bm *blockmanager.BlockManager) (*Writer, error) {

	// Kreiraj folder ako ne postoji
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err := os.MkdirAll(dirPath, 0755) // permisije nad fajlom
		if err != nil {
			return nil, fmt.Errorf("ne mogu da kreiram direktorijum: %v", err)
		}
	}

	maxIndex := FindMaxSegmentIndex(dirPath)
	var recordsInLast int
	segmentPath := filepath.Join(dirPath, fmt.Sprintf("wal_segment_%d.log", maxIndex))
	if maxIndex > 0 {
		// izracunaj broj validnih zapisa u tom segmentu
		//segmentPath := filepath.Join(dirPath, fmt.Sprintf("wal_segment_%d.log", maxIndex))
		records, err := ReadAllRecords(bm, segmentPath)
		if err != nil {
			return nil, fmt.Errorf("ne mogu da procitam zapise iz poslednjeg segmenta: %v", err)
		}
		recordsInLast = len(records)
	}

	w := &Writer{
		dirPath:       dirPath,
		segmentSize:   segmentSize,
		currentIndex:  maxIndex,
		recordsInFile: recordsInLast,
		blockManager:  bm,
	}
	w.currentSegmentPath = segmentPath
	return w, nil
}

// Funkcija za upisivanje zapisa i rotaciju
// Ispred reci func imamo (w *Writer) - ovo se naziva receiver - postavlja se pre naziva funkcije i oznacava koja struktura moze da poziva tu funkciju
func (w *Writer) Write(record Record) error {
	// Provera da li je predjen prag dozvoljenih parove kljuc-vr u wal-u
	if w.recordsInFile >= w.segmentSize {
		// rotacija na novi fajl
		w.currentIndex++
		w.recordsInFile = 0
	}

	segmentPath := fmt.Sprintf("%s/wal_segment_%d.log", w.dirPath, w.currentIndex) // printf je formatirani string, na mesta %s, i %d se ugradjuju prosledjene vrednosti respektivno
	w.currentSegmentPath = segmentPath

	blockNum := int64(w.recordsInFile)

	// Funkcija WriteRecord od gore
	err := WriteRecord(w.blockManager, segmentPath, blockNum, record)
	if err != nil {
		return err
	}

	w.recordsInFile++
	return nil
}

// Ucita sve zapise iz svih WAL segmenata u folderu, sortirano po redosledu
func LoadAllSegments(bm *blockmanager.BlockManager, dirPath string, recordsPerSegment int) ([]Record, error) {
	var records []Record // vracamo niz Record-a

	files, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("ne mogu da procitam sadrzaj foldera: %v", err)
	}

	var walFiles []string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "wal_segment_") && strings.HasSuffix(file.Name(), ".log") {
			walFiles = append(walFiles, file.Name())
		}
	}

	sort.Slice(walFiles, func(i, j int) bool {
		getNumber := func(name string) int {
			base := strings.TrimSuffix(strings.TrimPrefix(name, "wal_segment_"), ".log")
			n, _ := strconv.Atoi(base)
			return n
		}
		return getNumber(walFiles[i]) < getNumber(walFiles[j])
	})

	for _, fname := range walFiles {
		path := filepath.Join(dirPath, fname)

		// Umesto file open → citamo blokove
		recs, err := ReadAllRecords(bm, path)
		if err != nil {
			return nil, fmt.Errorf("greska u fajlu %s: %v", fname, err)
		}

		records = append(records, recs...)
	}

	return records, nil
}

// Funkcija koja se poziva sa namerom da se iskoristi ono za sta je WAL i napravljen
// Dakle ako bismo uradili par PUT operacija i nestane nam struje, ili mi samo uradimo EXIT da ugasimo bazu, a prethodno nismo sacuvali stanje ili nije izazvana flush ili nismo uradili SNAPSHOT, po pokretanju baze ponovo sve iz wal-a se ucitava u Memtable strukturu
func ReplayWAL(bm *blockmanager.BlockManager, walDir string, mt memtable.MemtableInterface) error {
	files, err := os.ReadDir(walDir)
	if err != nil {
		return fmt.Errorf("ne mogu da procitam WAL direktorijum: %v", err)
	}

	// trazimo sve fajlove sa nastavkom .log to su nasi wal segmenti
	var logFiles []string
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".log") {
			logFiles = append(logFiles, f.Name())
		}
	}

	//  Sortiramo WAL fajlove po imenu
	sort.Strings(logFiles)

	for _, name := range logFiles {
		fmt.Printf(" WAL segment za replay: %s\n", name)
		path := filepath.Join(walDir, name)

		// readAllRecord funkcija od gore
		records, err := ReadAllRecords(bm, path)
		if err != nil {
			return fmt.Errorf("ne mogu da procitam WAL zapis iz %s: %v", path, err)
		}

		for _, rec := range records {
			if rec.Tombstone {
				mt.Delete(string(rec.Key))
			} else {
				// u Memtable unosimo kljuc i vrednost iz wal segmenata po ponovnom pokretanju baze
				mt.Put(string(rec.Key), rec.Value)
			}
			// Debug info:
			fmt.Printf(" WAL unet u memtable: %s → %s\n", rec.Key, rec.Value)
		}
	}
	return nil
}

// Vraca trenutni aktivni WAL segment i broj zapisa u njemu
// Korisceno za kasnije CLI komande o stanju baze, nije preterano bitno
func (w *Writer) StateInfo() (string, int) {
	return filepath.Base(w.currentSegmentPath), w.currentIndex
}

func (w *Writer) GetCurrentSegmentPath() string {
	return w.currentSegmentPath
}

// Funkcija koja vraca maksimalni index do sada kreiranih WAL segmenata
func FindMaxSegmentIndex(dirPath string) int {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return 0
	}

	maxIndex := 0
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "wal_segment_") && strings.HasSuffix(file.Name(), ".log") {
			base := strings.TrimSuffix(strings.TrimPrefix(file.Name(), "wal_segment_"), ".log")
			n, err := strconv.Atoi(base)
			if err == nil && n > maxIndex {
				maxIndex = n
			}
		}
	}
	return maxIndex
}

func (w *Writer) SetCurrentIndex(index int) {
	w.currentIndex = index
}

func (w *Writer) GetCurrentIndex() int {
	return w.currentIndex
}
