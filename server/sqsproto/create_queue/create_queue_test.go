package create_queue

import "testing"

func TestSerialization(t *testing.T) {
	msg := &CreateQueueResponse{
		ResponseId: "123123",
		QueueUrl:   "http://123123.12312.12312/",
	}
	println(msg.XmlDocument())
}
