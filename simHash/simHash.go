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
	hashedWords, counts := splitAndHash(text)
	sum1 := getSum(hashedWords, counts)
	fmt.Print(sum1)

	fmt.Print("\nDruga tura\n")

	text2 := newText() //ucitavanje novog teksta
	hashedWords, counts = splitAndHash(text2)
	sum2 := getSum(hashedWords, counts)
	fmt.Print(sum2)

	fmt.Print("\nHamming distance")
	fmt.Print(hammingDistance(sum1, sum2))

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

func splitAndHash(text string) ([]string, map[string]int) {
	splitText := strings.Fields(text)
	hashedWords := make([]string, 0)
	counts := make(map[string]int)
	j := 0
	for i := 0; i < len(splitText); i++ {
		word := hashWord(splitText[i])
		counts[word]++
		if counts[word] == 1 {
			hashedWords = append(hashedWords, word)
			fmt.Println(word)
			j++
		}
	} //for petlja za hashovanje svake pojedinacne reci
	return hashedWords, counts
}

func hashWord(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	binaryHash := fmt.Sprintf("%032b", h.Sum32())
	return binaryHash
}

func getSum(hashedWords []string, counts map[string]int) string {
	value := make([]int, 32)
	for j := 0; j < 32; j++ {
		for _, word := range hashedWords {
			if string(word[j]) == "0" {
				value[j] -= counts[word]
			} else {
				value[j] += counts[word]
			}
		}
		if value[j] > 0 {
			value[j] = 1
		} else {
			value[j] = 0
		}
	}

	return fmt.Sprint(value)
}

func hammingDistance(s1 string, s2 string) string {
	result := make([]byte, 32)
	for i := 0; i < 32; i++ {
		result[i] = s1[i] ^ s2[i]
	}

	return fmt.Sprint(result)
}

//da li svejedno mnozimo li bitove sa brojem ponavljanja ili ih samo prolazimo ponovo
