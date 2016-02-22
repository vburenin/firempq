package send_message_batch

import (
	"encoding/xml"
	"firempq/server/sqsproto/send_message"
	"firempq/server/sqsproto/sqs_response"
	"firempq/server/sqsproto/sqsencoding"
	"firempq/server/sqsproto/sqserr"
	"firempq/server/sqsproto/urlutils"
	"firempq/services/pqueue"
	"net/http"
	"firempq/server/sqsproto/validation"
)

type SendMessageBatchResponse struct {
	XMLName     xml.Name      `xml:"http://queue.amazonaws.com/doc/2012-11-05/ SendMessageBatchResponse"`
	ErrorEntry  []interface{} `xml:"SendMessageBatchResult>BatchResultErrorEntry,omitempty"`
	ResultEntry []interface{} `xml:"SendMessageBatchResult>SendMessageBatchResultEntry,omitempty"`
	RequestId   string        `xml:"ResponseMetadata>RequestId"`
}

func (self *SendMessageBatchResponse) HttpCode() int                        { return http.StatusOK }
func (self *SendMessageBatchResponse) XmlDocument() string                  { return sqsencoding.EncodeXmlDocument(self) }
func (self *SendMessageBatchResponse) BatchResult(docId string) interface{} { return nil }

type MessageBatchParams struct {
	Id         string
	ParamsList []string
}

func NewReqQueueAttr() urlutils.ISubContainer { return &MessageBatchParams{} }

func (self *MessageBatchParams) Parse(paramName, value string) *sqserr.SQSError {
	switch paramName {
	case "Id":
		self.Id = value
	default:
		self.ParamsList = append(self.ParamsList, paramName, value)
	}
	return nil
}

func SendMessageBatch(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	attrs, _ := urlutils.ParseNNotationAttr(
		"SendMessageBatchRequestEntry.", sqsQuery.ParamsList, nil, NewReqQueueAttr)

	attrsLen := len(attrs)

	checker, err := validation.NewBatchIdValidation(attrsLen)
	if err != nil {
		return err
	}

	// map used to detect duplicated ids.
	attrList := make([]*MessageBatchParams, 0, attrsLen)

	for i := 1; i <= attrsLen; i++ {
		a, ok := attrs[i]
		if !ok {
			return sqserr.MissingParameterError("The request is missing sequence %d", i)
		}
		p, _ := a.(*MessageBatchParams)
		if err = checker.Validate(p.Id); err != nil {
			return err
		}
		if err := validation.ValidateBatchRequestId(p.Id); err != nil {
			return err
		}
		attrList = append(attrList, p)
	}

	batchResponse := &SendMessageBatchResponse{
		RequestId: "rreer",
	}

	for _, a := range attrList {
		resp := send_message.PushAMessage(pq, sqsQuery.SenderId, a.ParamsList)
		if resp.HttpCode() == 200 {
			batchResponse.ResultEntry = append(batchResponse.ResultEntry, resp.BatchResult(a.Id))
		} else {
			batchResponse.ErrorEntry = append(batchResponse.ErrorEntry, resp.BatchResult(a.Id))
		}
	}

	return batchResponse
}
