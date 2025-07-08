package sstable

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
)

// generisemo iz liste entry-ja
func GenerateMerkleRoot(entries []Entry) []byte {
	var hashes [][]byte

	for _, e := range entries {
		h := sha256.New()

		h.Write([]byte(e.Key))
		h.Write(e.Value)
		if e.Tombstone {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}

		tmp := make([]byte, 8)
		binary.LittleEndian.PutUint64(tmp, e.Timestamp)
		h.Write(tmp)

		hashes = append(hashes, h.Sum(nil))
	}

	for len(hashes) > 1 {
		var newLevel [][]byte
		for i := 0; i < len(hashes); i += 2 {
			//ako je neprarno onda kopiramo poslednji
			if i+1 == len(hashes) {
				newLevel = append(newLevel, hashes[i])
				break
			}
			//ako nije spajamo dva hasha
			h := sha256.New()
			h.Write(hashes[i])
			h.Write(hashes[i+1])
			newLevel = append(newLevel, h.Sum(nil))
		}
		hashes = newLevel
	}
	if len(hashes) == 1 {
		return hashes[0]
	}
	return nil
}

func SaveMerkleRoot(root []byte, dirPath string) error {
	f, err := os.Create(filepath.Join(dirPath, "merkle"))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write([]byte(hex.EncodeToString(root)))
	return err
}

func LoadMerkleRoot(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return hex.DecodeString(string(data))
}
