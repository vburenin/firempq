package snserr

import (
	"encoding/xml"
	"fmt"

	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
)

type SNSError struct {
	XMLName      xml.Name `xml:"http://sns.amazonaws.com/doc/2010-03-31/ ErrorResponse"`
	Type         string   `xml:"Error>Type"`
	Code         string   `xml:"Error>Code"`
	Message      string   `xml:"Error>Message"`
	Detail       string   `xml:"Error>Detail,omitempty"`
	RequestId    string   `xml:"RequestId"`
	HttpRespCode int      `xml:"-"`
}

func (self *SNSError) Error() string {
	return self.Code + ": " + self.Message
}

func (self *SNSError) XmlDocument() string {
	return sqs_response.EncodeXml(self)
}

func (self *SNSError) HttpCode() int {
	return self.HttpRespCode
}

func InvalidActionError(actionName string) *SNSError {
	return &SNSError{
		Code:         "InvalidAction",
		HttpRespCode: 400,
		Message:      fmt.Sprintf("The action %s is not valid for this endpoint.", actionName),
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func InvalidParameterError(msg string) *SNSError {
	return &SNSError{
		Code:         "InvalidParameter",
		HttpRespCode: 400,
		Message:      msg,
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func MalformedRequestError() *SNSError {
	return &SNSError{
		Code:         "MalformedRequest",
		HttpRespCode: 400,
		Message:      "Malformed request",
		Type:         "Sender",
		RequestId:    "reqid",
	}
}
