package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
)

type CountMinSketch struct {
	HashFunctions int     `json:"hash_functions"`
	Table         [][]int `json:"table"`
	Width         int     `json:"width"`
}

func NewCountMinSketch(hashFunctions, width int) *CountMinSketch {
	table := make([][]int, hashFunctions)
	for i := range table {
		table[i] = make([]int, width)
	}
	return &CountMinSketch{HashFunctions: hashFunctions, Table: table, Width: width}
}

func hash(value string, seed int) int {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%d-%s", seed, value)))
	return int(h.Sum32())
}

func (cms *CountMinSketch) Update(value string) {
	for i := 0; i < cms.HashFunctions; i++ {
		index := hash(value, i) % cms.Width
		cms.Table[i][index]++
	}
}

func (cms *CountMinSketch) Serialize(filename string) error {
	data, err := json.Marshal(cms)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filename, data, 0644)
}

func Deserialize(filename string) (*CountMinSketch, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var cms CountMinSketch
	if err := json.Unmarshal(data, &cms); err != nil {
		return nil, err
	}
	return &cms, nil
}
