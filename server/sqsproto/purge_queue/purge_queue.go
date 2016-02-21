package purge_queue

import (
	"encoding/xml"
	"firempq/server/sqsproto/sqs_response"
	"firempq/server/sqsproto/sqsencoding"
	"firempq/server/sqsproto/urlutils"
	"firempq/services/pqueue"
	"net/http"
)

type PurgeQueueResponse struct {
	XMLName   xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ PurgeQueueResponse"`
	RequestId string   `xml:"ResponseMetadata>RequestId"`
}

func (self *PurgeQueueResponse) HttpCode() int                        { return http.StatusOK }
func (self *PurgeQueueResponse) XmlDocument() string                  { return sqsencoding.EncodeXmlDocument(self) }
func (self *PurgeQueueResponse) BatchResult(docId string) interface{} { return nil }

func PurgeQueue(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	pq.Clear()
	return &PurgeQueueResponse{
		RequestId: "purgeid",
	}
}
