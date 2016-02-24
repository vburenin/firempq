package receive_message

import (
	"testing"
)

func TestReceiveMessage(t *testing.T) {
	attr1 := &SysMessageAttribute{
		Name:  "Some name",
		Value: "Some value",
	}
	attr2 := &SysMessageAttribute{
		Name:  "Some name2",
		Value: "Some value2",
	}
	msg1 := &MessageResponse{
		MessageId:              "123123123",
		ReceiptHandle:          "123123123123",
		MD5OfMessageBody:       "1231231231",
		MD5OfMessageAttributes: "12312312312312",
	}
	msg1.Attributes = append(msg1.Attributes, attr1, attr2)

	msg2 := &MessageResponse{
		MessageId:              "asdasdasdas",
		ReceiptHandle:          "asdasdasd",
		MD5OfMessageBody:       "asdasdsa",
		MD5OfMessageAttributes: "asdasdasd",
	}
	msg1.Attributes = msg1.Attributes

	resp := &ReceiveMessageResponse{
		RequestId: "1111-112312312-321312",
	}
	resp.Message = append(resp.Message, msg1, msg2)
	println(resp.XmlDocument())
}
