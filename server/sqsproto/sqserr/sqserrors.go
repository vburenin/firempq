package sqserr

import (
	"encoding/xml"
	"fmt"

	"github.com/vburenin/firempq/server/sqsproto/sqsencoding"
)

type SQSError struct {
	XMLName      xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ErrorResponse"`
	Type         string   `xml:"Error>Type"`
	Code         string   `xml:"Error>Code"`
	Message      string   `xml:"Error>Message"`
	Detail       string   `xml:"Error>Detail"`
	RequestId    string   `xml:"RequestId"`
	HttpRespCode int      `xml:"-"`
}

type BatchResultErrorEntry struct {
	Id          string
	SenderFault bool
	Message     string
	Code        string
}

func (self *SQSError) Error() string {
	return self.Code + ": " + self.Message
}

func (self *SQSError) XmlDocument() string {
	return sqsencoding.EncodeXmlDocument(self)
}

func (self *SQSError) BatchResult(docId string) interface{} {
	return &BatchResultErrorEntry{
		Id:          docId,
		SenderFault: self.Type == "Sender",
		Message:     self.Message,
		Code:        self.Code,
	}
}

func (self *SQSError) HttpCode() int {
	return self.HttpRespCode
}

func MalformedInputError(msg string) *SQSError {
	return &SQSError{
		Code:         "MalformedInput",
		HttpRespCode: 400,
		Message:      msg,
		RequestId:    "reqid",
		Type:         "Sender",
	}
}

func EmptyValueError(msg string) *SQSError {
	return &SQSError{
		Code:         "EmptyValue",
		HttpRespCode: 400,
		Message:      msg,
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func InvalidAttributeNameError(msg string, params ...interface{}) *SQSError {
	return &SQSError{
		Code:         "InvalidAttributeName",
		HttpRespCode: 400,
		Message:      fmt.Sprintf(msg, params...),
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func InvalidActionError(actionName string) *SQSError {
	return &SQSError{
		Code:         "InvalidAction",
		HttpRespCode: 400,
		Message:      fmt.Sprintf("The action %s is not valid for this endpoint.", actionName),
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func ServiceDeniedError() *SQSError {
	return &SQSError{
		Code:         "InvalidAction",
		HttpRespCode: 403,
		Message:      fmt.Sprintf("Unable to determine service/operation name to be authorized"),
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func InvalidQueueNameError() *SQSError {
	return &SQSError{
		Code:         "InvalidParameterValue",
		HttpRespCode: 400,
		Message:      "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length",
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func QueueDoesNotExist() *SQSError {
	return &SQSError{
		Code:         "AWS.SimpleQueueService.NonExistentQueue",
		HttpRespCode: 400,
		Message:      "The specified queue does not exist for this wsdl version.",
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func QueueAlreadyExistsError(msg string) *SQSError {
	return &SQSError{
		Code:         "QueueAlreadyExists",
		HttpRespCode: 400,
		Message:      msg,
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func ServerSideError(msg string) *SQSError {
	return &SQSError{
		Code:         "InternalFailure",
		HttpRespCode: 500,
		Message:      msg,
		Type:         "Server",
		RequestId:    "reqid",
	}
}

func InvalidParameterValueError(msg string, params ...interface{}) *SQSError {
	return &SQSError{
		Code:         "InvalidParameterValue",
		HttpRespCode: 400,
		Message:      fmt.Sprintf(msg, params...),
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func InvalidAttributeValueError(msg string, params ...interface{}) *SQSError {
	return &SQSError{
		Code:         "InvalidAttributeValue",
		HttpRespCode: 400,
		Message:      fmt.Sprintf(msg, params...),
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func MissingParameterError(msg string, params ...interface{}) *SQSError {
	return &SQSError{
		Code:         "MissingParameter",
		HttpRespCode: 400,
		Message:      fmt.Sprintf(msg, params...),
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func InvalidReceiptHandleError(msg string) *SQSError {
	return &SQSError{
		Code:         "ReceiptHandleIsInvalid",
		HttpRespCode: 404,
		Message:      msg,
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func Error400(code, msg string, params ...interface{}) *SQSError {
	return &SQSError{
		Code:         code,
		HttpRespCode: 400,
		Message:      fmt.Sprintf(msg, params...),
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func EmptyBatchRequestError() *SQSError {
	return &SQSError{
		Code:         "AWS.SimpleQueueService.EmptyBatchRequest",
		HttpRespCode: 400,
		Message:      "There should be at least one request entry in the request.",
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func TooManyEntriesInBatchRequestError() *SQSError {
	return &SQSError{
		Code:         "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
		HttpRespCode: 400,
		Message:      "Maximum number of entries per request is 10",
		Type:         "Sender",
		RequestId:    "reqid",
	}
}

func NotDistinctIdsError(id string) *SQSError {
	return &SQSError{
		Code:         "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
		HttpRespCode: 400,
		Message:      fmt.Sprintf("Id %s repeated.", id),
		Type:         "Sender",
		RequestId:    "reqid",
	}
}
