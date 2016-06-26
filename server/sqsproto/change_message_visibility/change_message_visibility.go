package change_message_visibility

import (
	"encoding/xml"
	"net/http"
	"strconv"

	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
	"github.com/vburenin/firempq/server/sqsproto/sqserr"
	"github.com/vburenin/firempq/server/sqsproto/urlutils"
)

type ChangeMessageVisibilityResponse struct {
	XMLName   xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ChangeMessageVisibilityResponse"`
	RequestId string   `xml:"ResponseMetadata>RequestId"`
}

func (self *ChangeMessageVisibilityResponse) HttpCode() int { return http.StatusOK }
func (self *ChangeMessageVisibilityResponse) XmlDocument() string {
	return sqs_response.EncodeXml(self)
}
func (self *ChangeMessageVisibilityResponse) BatchResult(docId string) interface{} { return nil }

func ChangeMessageVisibility(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	var receipt string
	var visibilityTimeout int64 = -1
	paramsLen := len(sqsQuery.ParamsList) - 1

	for i := 0; i < paramsLen; i += 2 {
		switch sqsQuery.ParamsList[i] {
		case "ReceiptHandle":
			receipt = sqsQuery.ParamsList[i+1]
		case "VisibilityTimeout":
			v, err := strconv.ParseInt(sqsQuery.ParamsList[i+1], 10, 0)
			if err != nil || v < 0 {
				return sqserr.MalformedInputError("VisibilityTimeout must be a positive integer value")
			}
			visibilityTimeout = v * 1000
		}
	}

	if receipt == "" {
		return sqserr.MissingParameterError("The request must contain the parameter ReceiptHandle.")
	}

	if visibilityTimeout < 0 {
		return sqserr.MissingParameterError("The request must contain the parameter VisibilityTimeout.")
	}

	resp := pq.UpdateLockByRcpt(receipt, visibilityTimeout)
	if resp == mpqerr.ERR_INVALID_RECEIPT {
		return sqserr.InvalidReceiptHandleError("The input receipt handle is not a valid receipt handle.")
	}

	return &ChangeMessageVisibilityResponse{
		RequestId: "cvtreq",
	}
}
