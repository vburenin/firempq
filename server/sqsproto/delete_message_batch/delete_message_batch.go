package delete_message_batch

import (
	"encoding/xml"
	"firempq/errors"
	"firempq/server/sqsproto/sqs_response"
	"firempq/server/sqsproto/sqsencoding"
	"firempq/server/sqsproto/sqserr"
	"firempq/server/sqsproto/urlutils"
	"firempq/server/sqsproto/validation"
	"firempq/services/pqueue"
	"net/http"
)

type DeleteMessageBatchResponse struct {
	XMLName     xml.Name      `xml:"http://queue.amazonaws.com/doc/2012-11-05/ DeleteMessageBatchResponse"`
	ErrorEntry  []interface{} `xml:"DeleteMessageBatchResult>BatchResultErrorEntry,omitempty"`
	ResultEntry []interface{} `xml:"DeleteMessageBatchResult>DeleteMessageBatchResultEntry,omitempty"`
	RequestId   string        `xml:"ResponseMetadata>RequestId"`
}

type OkDelete struct {
	Id string `xml:"Id"`
}

func (self *DeleteMessageBatchResponse) HttpCode() int { return http.StatusOK }
func (self *DeleteMessageBatchResponse) XmlDocument() string {
	return sqsencoding.EncodeXmlDocument(self)
}
func (self *DeleteMessageBatchResponse) BatchResult(docId string) interface{} { return nil }

type DeleteBatchParams struct {
	Id            string
	ReceiptHandle string
}

func (self *DeleteBatchParams) Parse(paramName, value string) *sqserr.SQSError {
	switch paramName {
	case "Id":
		self.Id = value
	case "ReceiptHandle":
		self.ReceiptHandle = value
	}
	return nil
}

func NewDeleteBatchParams() urlutils.ISubContainer { return &DeleteBatchParams{} }

func DeleteMessageBatch(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	attrs, _ := urlutils.ParseNNotationAttr(
		"DeleteMessageBatchRequestEntry.", sqsQuery.ParamsList, nil, NewDeleteBatchParams)
	attrsLen := len(attrs)

	checker, err := validation.NewBatchIdValidation(attrsLen)
	if err != nil {
		return err
	}

	attrList := make([]*DeleteBatchParams, 0, attrsLen)

	for i := 1; i <= attrsLen; i++ {
		a, ok := attrs[i]
		if !ok {
			return sqserr.MissingParameterError("The request is missing sequence %d", i)
		}
		p, _ := a.(*DeleteBatchParams)
		if err = checker.Validate(p.Id); err != nil {
			return err
		}
		attrList = append(attrList, p)
	}

	output := &DeleteMessageBatchResponse{
		RequestId: "delbatch",
	}

	for _, batchItem := range attrList {
		resp := pq.DeleteByReceipt(batchItem.ReceiptHandle)
		if resp == errors.ERR_INVALID_RECEIPT {
			e := sqserr.InvalidReceiptHandleError("The input receipt handle is not a valid receipt handle.")
			output.ErrorEntry = append(output.ErrorEntry, e.BatchResult(batchItem.Id))
		} else {
			output.ResultEntry = append(output.ResultEntry, &OkDelete{Id: batchItem.Id})
		}
	}
	return output
}
