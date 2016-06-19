package change_message_visibility_batch

import (
	"encoding/xml"
	"net/http"
	"strconv"

	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
	"github.com/vburenin/firempq/server/sqsproto/sqsencoding"
	"github.com/vburenin/firempq/server/sqsproto/sqserr"
	"github.com/vburenin/firempq/server/sqsproto/urlutils"
	"github.com/vburenin/firempq/server/sqsproto/validation"
)

type ChangeMessageVisibilityBatchResponse struct {
	XMLName     xml.Name      `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ChangeMessageVisibilityBatchResponse"`
	ErrorEntry  []interface{} `xml:"ChangeMessageVisibilityBatchResult>BatchResultErrorEntry,omitempty"`
	ResultEntry []interface{} `xml:"ChangeMessageVisibilityBatchResult>ChangeMessageVisibilityBatchResultEntry,omitempty"`
	RequestId   string        `xml:"ResponseMetadata>RequestId"`
}

type OkChange struct {
	Id string `xml:"Id"`
}

func (self *ChangeMessageVisibilityBatchResponse) HttpCode() int { return http.StatusOK }
func (self *ChangeMessageVisibilityBatchResponse) XmlDocument() string {
	return sqsencoding.EncodeXmlDocument(self)
}
func (self *ChangeMessageVisibilityBatchResponse) BatchResult(docId string) interface{} { return nil }

type VisibilityBatchParams struct {
	Id                string
	ReceiptHandle     string
	VisibilityTimeout int64
}

func (self *VisibilityBatchParams) Parse(paramName, value string) *sqserr.SQSError {
	switch paramName {
	case "Id":
		self.Id = value
	case "ReceiptHandle":
		self.ReceiptHandle = value
	case "VisibilityTimeout":
		v, err := strconv.ParseInt(value, 10, 0)
		if err != nil || v < 0 {
			return sqserr.MalformedInputError("VisibilityTimeout must be a positive integer value")
		}
		self.VisibilityTimeout = v * 1000
	}
	return nil
}

func NewVisibilityBatchParams() urlutils.ISubContainer { return &VisibilityBatchParams{} }

func ChangeMessageVisibilityBatch(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	attrs, _ := urlutils.ParseNNotationAttr(
		"ChangeMessageVisibilityBatchRequestEntry.", sqsQuery.ParamsList,
		nil, NewVisibilityBatchParams)
	attrsLen := len(attrs)

	checker, err := validation.NewBatchIdValidation(attrsLen)
	if err != nil {
		return err
	}

	attrList := make([]*VisibilityBatchParams, 0, attrsLen)

	for i := 1; i <= attrsLen; i++ {
		a, ok := attrs[i]
		if !ok {
			return sqserr.MissingParameterError("The request is missing sequence %d", i)
		}
		p, _ := a.(*VisibilityBatchParams)
		if err = checker.Validate(p.Id); err != nil {
			return err
		}
		attrList = append(attrList, p)
	}

	output := &ChangeMessageVisibilityBatchResponse{
		RequestId: "visibilitybatch",
	}

	for _, batchItem := range attrList {
		resp := pq.UpdateLockByRcpt(batchItem.ReceiptHandle, batchItem.VisibilityTimeout)
		if resp == mpqerr.ERR_INVALID_RECEIPT {
			e := sqserr.InvalidReceiptHandleError("The input receipt handle is not a valid receipt handle.")
			output.ErrorEntry = append(output.ErrorEntry, e.BatchResult(batchItem.Id))
		} else {
			output.ResultEntry = append(output.ResultEntry, &OkChange{Id: batchItem.Id})
		}
	}
	return output
}
