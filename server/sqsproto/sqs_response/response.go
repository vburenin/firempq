package sqs_response

type SQSResponse interface {
	XmlDocument() string
	HttpCode() int
	BatchResult(docId string) interface{}
}
