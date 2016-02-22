package delete_queue

import (
	"encoding/xml"
	"net/http"

	"firempq/errors"
	"firempq/server/sqsproto/sqs_response"
	"firempq/server/sqsproto/sqsencoding"
	"firempq/server/sqsproto/sqserr"
	"firempq/server/sqsproto/urlutils"
	"firempq/services"
)

type DeleteQueueResponse struct {
	XMLName   xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ DeleteQueueResponse"`
	RequestId string   `xml:"ResponseMetadata>RequestId"`
}

func (self *DeleteQueueResponse) HttpCode() int { return http.StatusOK }
func (self *DeleteQueueResponse) XmlDocument() string {
	return sqsencoding.EncodeXmlDocument(self)
}
func (self *DeleteQueueResponse) BatchResult(docId string) interface{} { return nil }

func DeleteQueue(svcMgr *services.ServiceManager, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	resp := svcMgr.DropService(sqsQuery.QueueName)
	if resp == errors.ERR_NO_SVC {
		return sqserr.QueueDoesNotExist()
	}
	return &DeleteQueueResponse{
		RequestId: "delqueue",
	}
}
