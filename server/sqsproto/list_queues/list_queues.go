package list_queues

import (
	"encoding/xml"
	"firempq/server/sqsproto/sqs_response"
	"firempq/server/sqsproto/sqsencoding"
	"firempq/server/sqsproto/urlutils"
	"firempq/services"
	"net/http"
	"sort"
)

type ListQueuesResponse struct {
	XMLName   xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ListQueuesResponse"`
	QueueUrl  []string `xml:"ListQueuesResponse>QueueUrl"`
	RequestId string   `xml:"ListQueuesResponse>ResponseMetadata>RequestId"`
}

func (self *ListQueuesResponse) HttpCode() int                        { return http.StatusOK }
func (self *ListQueuesResponse) XmlDocument() string                  { return sqsencoding.EncodeXmlDocument(self) }
func (self *ListQueuesResponse) BatchResult(docId string) interface{} { return nil }

func ListQueues(svcMgr *services.ServiceManager, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
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
