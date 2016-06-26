package remove_permission

import (
	"encoding/xml"
	"net/http"

	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
	"github.com/vburenin/firempq/server/sqsproto/urlutils"
)

type RemovePermissionResponse struct {
	XMLName   xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ RemovePermissionResponse"`
	RequestId string   `xml:"ResponseMetadata>RequestId"`
}

func (s *RemovePermissionResponse) XmlDocument() string                  { return sqs_response.EncodeXml(s) }
func (s *RemovePermissionResponse) HttpCode() int                        { return http.StatusOK }
func (s *RemovePermissionResponse) BatchResult(docId string) interface{} { return nil }

func RemovePermission(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	return &RemovePermissionResponse{
		RequestId: "req",
	}
}
