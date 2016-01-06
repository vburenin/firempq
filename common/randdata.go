package common

import (
	"math/rand"
	"sync"
	"time"

	. "firempq/utils"
)

const (
	msgIdCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	msgIdLength     = 24
)

var msgIdCharsTotal = int64(len(msgIdCharacters))

type IdGen struct {
	mutex  sync.Mutex
	rndgen rand.Source
}

// NewIdGen return new Message ID generator object to be used within the scope of one queue.
func NewIdGen() *IdGen {
	return &IdGen{
		rndgen: rand.NewSource(time.Now().UnixNano()),
	}
}

// GenRandId generates random message id from the set of allowed characters.
func (g *IdGen) GenRandId() string {
	randData := make([]byte, msgIdLength)
	g.mutex.Lock()
	randData[0] = '_'
	for i := 1; i < msgIdLength; i++ {
		randData[i] = msgIdCharacters[g.rndgen.Int63()%msgIdCharsTotal]
	}
	g.mutex.Unlock()
	return UnsafeBytesToString(randData)
}
