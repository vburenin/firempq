package delete_queue

import (
	"encoding/xml"
	"net/http"

	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/qmgr"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
	"github.com/vburenin/firempq/server/sqsproto/sqserr"
	"github.com/vburenin/firempq/server/sqsproto/urlutils"
)

type DeleteQueueResponse struct {
	XMLName   xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ DeleteQueueResponse"`
	RequestId string   `xml:"ResponseMetadata>RequestId"`
}

func (self *DeleteQueueResponse) HttpCode() int { return http.StatusOK }
func (self *DeleteQueueResponse) XmlDocument() string {
	return sqs_response.EncodeXml(self)
}
func (self *DeleteQueueResponse) BatchResult(docId string) interface{} { return nil }

func DeleteQueue(svcMgr *qmgr.ServiceManager, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	resp := svcMgr.DropService(sqsQuery.QueueName)
	if resp == mpqerr.ERR_NO_SVC {
		return sqserr.QueueDoesNotExist()
	}
	return &DeleteQueueResponse{
		RequestId: "delqueue",
	}
}
