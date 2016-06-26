package add_permission

import (
	"encoding/xml"
	"net/http"

	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
	"github.com/vburenin/firempq/server/sqsproto/urlutils"
)

type AddPermissionResponse struct {
	XMLName   xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ AddPermissionResponse"`
	RequestId string   `xml:"ResponseMetadata>RequestId"`
}

func (s *AddPermissionResponse) XmlDocument() string                  { return sqs_response.EncodeXml(s) }
func (s *AddPermissionResponse) HttpCode() int                        { return http.StatusOK }
func (s *AddPermissionResponse) BatchResult(docId string) interface{} { return nil }

func AddPermission(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	return &AddPermissionResponse{
		RequestId: "req",
	}
}
