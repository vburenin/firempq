package idgen

import (
	"math/rand"
	"sync"
	"time"
)

const (
	msgIdCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_."
	msgIdLength     = 16
)

var msgIdCharsTotal = int64(len(msgIdCharacters))

type IdGen struct {
	mutex  sync.Mutex
	rndgen rand.Source
}

// NewGen return new Message ID generator object to be used within the scope of one queue.
func NewGen() *IdGen {
	return &IdGen{
		rndgen: rand.NewSource(time.Now().UnixNano()),
	}
}

// RandId generates random message id from the set of allowed characters.
func (g *IdGen) RandId() string {
	randData := make([]byte, msgIdLength)
	randData[0] = '_'
	g.mutex.Lock()
	for i := 1; i < msgIdLength; i++ {
		randData[i] = msgIdCharacters[byte(g.rndgen.Int63())>>2]
	}
	g.mutex.Unlock()
	return string(randData)
}
