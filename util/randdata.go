package util

import "math/rand"

const (
	MSG_ID_CHARACTERS   = "abcdefghijklmnopqrstuvwxyz0123456789"
	MSG_ID_LENGTH       = 24
	MSG_ID_CHARS_LENGTH = 36
)

func GenRandMsgId() string {
	randData := make([]byte, MSG_ID_LENGTH)
	for i := 0; i < 16; i++ {
		randData[i] = MSG_ID_CHARACTERS[rand.Int()%MSG_ID_CHARS_LENGTH]
	}
	return string(randData)
}
