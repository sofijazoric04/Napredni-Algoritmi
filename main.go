package main

import (
	"napredni/cli"
	"napredni/config"
	"napredni/kvengine"
)

func main() {
	//  Ucitaj konfiguraciju
	err := config.LoadConfig("config.json")
	if err != nil {
		panic("Konfiguracija nije učitana: " + err.Error())
	}

	//  Napravi engine
	engine := kvengine.NewEngine(config.Current.MemtableMaxEntries, "data/wal", "data/sstables")
	cli.Start(engine)
}
