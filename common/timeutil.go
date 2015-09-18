package common

import (
	"time"
)

func uts() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func UnixNanoTs() int64 {
	return time.Now().UnixNano()
}

var testValue int64 = uts()
var origUtc func() int64 = uts
var Uts func() int64 = uts

func IncTimer(inc int64) {
	testValue += inc
}

func EnableTesting() {
	testValue = Uts()
	Uts = func() int64 { return testValue }
}

func DisableTesting() {
	Uts = origUtc
}
