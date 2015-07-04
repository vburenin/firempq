package pqueue

import (
	"testing"
)

func TestBinary(t *testing.T) {
	msg := NewPQMessageWithId("123", 12)
	msg.PopCount = 33
	msg.UnlockTs = 987654321

	data := msg.ToBinary()

	msgFrom := PQMessageFromBinary("123", data)

	if msgFrom.UnlockTs != msg.UnlockTs ||
		msgFrom.PopCount != msg.PopCount ||
		msgFrom.Priority != msg.Priority ||
		msgFrom.CreatedTs != msg.CreatedTs {
		t.Error("Data is incorrect!")
	}

}
