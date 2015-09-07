package common

import (
	"math/rand"
	"sync"
	"time"
)

var rndgen = rand.NewSource(time.Now().UnixNano())
var rndMutex = sync.Mutex{}

const (
	MSG_ID_CHARACTERS   = "abcdefghijklmnopqrstuvwxyz0123456789"
	MSG_ID_LENGTH       = 24
	MSG_ID_CHARS_LENGTH = 36
)

func GenRandMsgId() string {
	randData := make([]byte, MSG_ID_LENGTH)
	rndMutex.Lock()
	defer rndMutex.Unlock()
	for i := 0; i < MSG_ID_LENGTH; i++ {
		randData[i] = MSG_ID_CHARACTERS[rndgen.Int63()%MSG_ID_CHARS_LENGTH]
	}
	return string(randData)
}

func GenRandMsgIdBytes() []byte {
	rndMutex.Lock()
	defer rndMutex.Unlock()

	randData := make([]byte, MSG_ID_LENGTH)
	for i := 0; i < MSG_ID_LENGTH; i++ {
		randData[i] = MSG_ID_CHARACTERS[rndgen.Int63()%MSG_ID_CHARS_LENGTH]
	}
	return randData
}
