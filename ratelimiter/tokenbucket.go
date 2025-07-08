package ratelimiter

import (
	"encoding/gob"
	"os"
	"sync"
	"time"
)

//  Tokenbucket služiće nam da ograničimo koliko puta korisnik može da pozove funkcije PUT, GET, DELETE
//  Zadajemo odredjeni broj tokena (poziva funkcija) i vreme u ms za koliko će se broj tokena povećati
//  Postoji default rate limiter, stavljeno 100 tokena i refill je za na primer sekundu
//  Korisnik može da override-uje ovaj default time pozivom SET_RATE_LIMIT

type TokenBucket struct {
	MaxTokens  int // maksimalno tokena
	Tokens     int // trenutno dostupni tokeni
	RefillRate time.Duration
	LastRefill time.Time // poslednje punjenje
	lock       sync.Mutex
}

// Funkcija koja kreira novi token bucket sa zadatim parametrima
func NewTokenBucket(maxTokens int, refillMillis int) *TokenBucket {
	return &TokenBucket{
		MaxTokens:  maxTokens,
		Tokens:     maxTokens, // inicijalno je kanta puna
		RefillRate: time.Duration(refillMillis) * time.Millisecond,
		LastRefill: time.Now(),
	}

}

// Proveravamo da li korisnik može da pošalje zahtev (ako ima dovoljno tokena)
func (tb *TokenBucket) Allow() bool {

	// Zaključavamo mutex da bismo sprečili konkurentni pristup
	tb.lock.Lock()
	defer tb.lock.Unlock()

	// 1. Računamo koliko je vremena prošlo od poslednjeg refilla
	now := time.Now()
	elapsed := now.Sub(tb.LastRefill)

	// 2. Dodajemo tokene, vodeći računa o maksimalnom broju tokena
	tokensToAdd := int(elapsed / tb.RefillRate)
	if tokensToAdd > 0 {
		tb.Tokens = min(tb.MaxTokens, tb.Tokens+tokensToAdd)
		tb.LastRefill = now
	}

	if tb.Tokens > 0 {
		tb.Tokens--
		return true
	}

	// Ako nema dovoljno tokena, vraćamo false, rate limit je prekoračen
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// SaveToFile serijalizuje trenutno stanje TokenBucket-a u fajl
func (tb *TokenBucket) SaveToFile(path string) error {
	tb.lock.Lock()
	defer tb.lock.Unlock()

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(tb)
}

// LoadFromFile deserijalizuje TokenBucket iz fajla
func LoadFromFile(path string) (*TokenBucket, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var tb TokenBucket
	decorder := gob.NewDecoder(file)
	err = decorder.Decode(&tb)
	if err != nil {
		return nil, err
	}

	return &tb, nil
}
