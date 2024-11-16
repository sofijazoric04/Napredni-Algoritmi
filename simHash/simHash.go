package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"strings"
)

func main() {
	text := newText() //ucitavanje novog teksta
	splitText := strings.Split(text, " ")
	hashedWords := make([]int, len(splitText))
	for i := 0; i < len(splitText); i++ {
		hashedWords[i] = int(hashWord(splitText[i]))
		fmt.Print(hashedWords[i])
		fmt.Print("\n")
	} //for petlja za hashovanje svake pojedinacne reci
}

func newText() string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter some text: ")
	text, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Došlo je do greške pri čitanju:", err)
		return ""
	}

	return text
}

func hashWord(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
