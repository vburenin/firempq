package delete_message

import (
	"encoding/xml"
	"net/http"

	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
	"github.com/vburenin/firempq/server/sqsproto/sqserr"
	"github.com/vburenin/firempq/server/sqsproto/urlutils"
)

type DeleteMessageResponse struct {
	XMLName   xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ DeleteMessageResponse"`
	RequestId string   `xml:"ResponseMetadata>RequestId"`
}

func (self *DeleteMessageResponse) HttpCode() int                        { return http.StatusOK }
func (self *DeleteMessageResponse) XmlDocument() string                  { return sqs_response.EncodeXml(self) }
func (self *DeleteMessageResponse) BatchResult(docId string) interface{} { return nil }

func DeleteMessage(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	var receipt string
	paramsLen := len(sqsQuery.ParamsList) - 1

	for i := 0; i < paramsLen; i += 2 {
		if sqsQuery.ParamsList[i] == "ReceiptHandle" {
			receipt = sqsQuery.ParamsList[i+1]
		}
	}

	if receipt == "" {
		return sqserr.MissingParameterError("The request must contain the parameter ReceiptHandle.")
	}

	resp := pq.DeleteByReceipt(receipt)
	if resp == mpqerr.ErrInvalidRcpt {
		return sqserr.InvalidReceiptHandleError("The input receipt handle is not a valid receipt handle.")
	}

	return &DeleteMessageResponse{
		RequestId: "delreq",
	}
}
