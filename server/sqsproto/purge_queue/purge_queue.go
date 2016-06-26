package purge_queue

import (
	"encoding/xml"
	"net/http"

	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
	"github.com/vburenin/firempq/server/sqsproto/urlutils"
)

type PurgeQueueResponse struct {
	XMLName   xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ PurgeQueueResponse"`
	RequestId string   `xml:"ResponseMetadata>RequestId"`
}

func (self *PurgeQueueResponse) HttpCode() int                        { return http.StatusOK }
func (self *PurgeQueueResponse) XmlDocument() string                  { return sqs_response.EncodeXml(self) }
func (self *PurgeQueueResponse) BatchResult(docId string) interface{} { return nil }

func PurgeQueue(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	pq.Clear()
	return &PurgeQueueResponse{
		RequestId: "purgeid",
	}
}
