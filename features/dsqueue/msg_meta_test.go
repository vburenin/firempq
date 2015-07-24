package dsqueue

import (
	"testing"
)

func TestBinary(t *testing.T) {
	msg := NewDSQMessageWithId("123")
	msg.PopCount = 33
	msg.UnlockTs = 987654321
	msg.DeliveryTs  = 987654322
	msg.pushAt = 1

	data := msg.ToBinary()

	msgFrom := PQMessageFromBinary("123", data)

	if msgFrom.UnlockTs != msg.UnlockTs ||
		msgFrom.PopCount != msg.PopCount ||
		msgFrom.DeliveryTs != msg.DeliveryTs ||
		msgFrom.pushAt != msg.pushAt ||
		msgFrom.CreatedTs != msg.CreatedTs {
		t.Error("Data is incorrect!")
	}
}
