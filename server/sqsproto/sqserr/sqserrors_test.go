package sqserr

import "testing"

func TestSerialization(t *testing.T) {
	msg := MalformedInputError("data")
	println(msg.XmlDocument())
}
