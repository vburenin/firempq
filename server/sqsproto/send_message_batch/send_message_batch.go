package send_message_batch

import (
	"encoding/xml"
	"firempq/server/sqsproto/send_message"
	"firempq/server/sqsproto/sqs_response"
	"firempq/server/sqsproto/sqsencoding"
	"firempq/server/sqsproto/sqserr"
	"firempq/server/sqsproto/urlutils"
	"firempq/server/sqsproto/validation"
	"firempq/services/pqueue"
	"net/http"
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
	if attrsLen == 0 {
		return sqserr.EmptyBatchRequestError()
	}

	if attrsLen > 10 {
		return sqserr.TooManyEntriesInBatchRequestError()
	}

	// map used to detect duplicated ids.
	idsMap := make(map[string]struct{}, attrsLen)
	attrList := make([]*MessageBatchParams, 0, attrsLen)

	for i := 1; i <= attrsLen; i++ {
		a, ok := attrs[i]
		if !ok {
			return sqserr.MissingParameterError("The request is missing sequence %d", i)
		}
		p, _ := a.(*MessageBatchParams)
		attrList = append(attrList, p)
		if err := validation.ValidateBatchRequestId(p.Id); err != nil {
			return err
		}
		if _, ok := idsMap[p.Id]; ok {
			return sqserr.NotDistinctIdsError(p.Id)
		}
		idsMap[p.Id] = struct{}{}
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
