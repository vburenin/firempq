package list_queues

import (
	"encoding/xml"
	"net/http"
	"sort"

	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
	"github.com/vburenin/firempq/server/sqsproto/urlutils"
)

type ListQueuesResponse struct {
	XMLName   xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ListQueuesResponse"`
	QueueUrl  []string `xml:"ListQueuesResult>QueueUrl"`
	RequestId string   `xml:"ResponseMetadata>RequestId"`
}

func (lqr *ListQueuesResponse) HttpCode() int                        { return http.StatusOK }
func (lqr *ListQueuesResponse) XmlDocument() string                  { return sqs_response.EncodeXml(lqr) }
func (lqr *ListQueuesResponse) BatchResult(docId string) interface{} { return nil }

func ListQueues(svcMgr *pqueue.QueueManager, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	nameList := svcMgr.GetQueueList(sqsQuery.QueueNamePrefix)
	sort.Strings(nameList)

	urlList := make([]string, 0, len(nameList))
	for _, name := range nameList {
		urlList = append(urlList, sqsQuery.Host+"/queue/"+name)
	}
	return &ListQueuesResponse{
		QueueUrl:  urlList,
		RequestId: "reqId",
	}
}
