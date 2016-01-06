package utils

import (
	"time"
)

func Uts() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
