package list_queues

import (
	"encoding/xml"
	"net/http"
	"sort"

	"github.com/vburenin/firempq/qmgr"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
	"github.com/vburenin/firempq/server/sqsproto/sqsencoding"
	"github.com/vburenin/firempq/server/sqsproto/urlutils"
)

type ListQueuesResponse struct {
	XMLName   xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ListQueuesResponse"`
	QueueUrl  []string `xml:"ListQueuesResponse>QueueUrl"`
	RequestId string   `xml:"ListQueuesResponse>ResponseMetadata>RequestId"`
}

func (self *ListQueuesResponse) HttpCode() int                        { return http.StatusOK }
func (self *ListQueuesResponse) XmlDocument() string                  { return sqsencoding.EncodeXmlDocument(self) }
func (self *ListQueuesResponse) BatchResult(docId string) interface{} { return nil }

func ListQueues(svcMgr *qmgr.ServiceManager, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	nameList := svcMgr.BuildServiceNameList(sqsQuery.QueueNamePrefix)
	sort.Strings(nameList)

	urlList := make([]string, 0, len(nameList))
	for _, name := range nameList {
		urlList = append(urlList, sqsQuery.Host+"/queue/"+name)
	}
	return &ListQueuesResponse{
		QueueUrl:  urlList,
		RequestId: "13123",
	}
}
