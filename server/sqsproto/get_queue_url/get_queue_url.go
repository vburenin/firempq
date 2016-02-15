package get_queue_url

import (
	"encoding/xml"

	"firempq/common"
	"firempq/server/sqsproto/sqs_response"
	"firempq/server/sqsproto/sqsencoding"
	"firempq/server/sqsproto/sqserr"
	"firempq/server/sqsproto/urlutils"
	"firempq/services"
	"net/http"
)

type GetQueueUrlResult struct {
	XMLName   xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ GetQueueUrlResult"`
	QueueUrl  string   `xml:"CreateQueueResult>QueueUrl"`
	RequestId string   `xml:"ResponseMetadata>RequestId"`
}

func (self *GetQueueUrlResult) XmlDocument() string { return sqsencoding.EncodeXmlDocument(self) }
func (self *GetQueueUrlResult) HttpCode() int       { return http.StatusOK }

func ParseMessageAttributes(sqsQuery *urlutils.SQSQuery) (map[string]string, *sqserr.SQSError) {
	for {

	}
}

func GetQueueUrl(svcMgr *services.ServiceManager, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	svc, ok := svcMgr.GetService(sqsQuery.QueueName)
	if !ok {
		return sqserr.QueueDoesNotExist()
	}
	if svc.GetTypeName() != common.STYPE_PRIORITY_QUEUE {
		return sqserr.QueueDoesNotExist()
	}

	return &GetQueueUrlResult{
		QueueUrl:  sqsQuery.Host + "/queue/" + sqsQuery.QueueName,
		RequestId: "1111-2222-3333",
	}
}
