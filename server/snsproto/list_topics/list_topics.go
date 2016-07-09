package list_topics

import (
	"encoding/xml"
	"net/http"

	"github.com/vburenin/firempq/server/snsproto/sns_query"
	"github.com/vburenin/firempq/server/snsproto/sns_response"
	"github.com/vburenin/firempq/server/snsproto/snsdefs"
	"github.com/vburenin/firempq/server/snsproto/tmgr"
)

type TopicArn struct {
	Arn string `xml:"TopicArn"`
}

type ListTopicsResponse struct {
	XMLName   xml.Name
	TopicArns []TopicArn `xml:"ListTopicsResult>Topics>member"`
	RequestId string     `xml:"ResponseMetadata>RequestId"`
}

func (s *ListTopicsResponse) XmlDocument() string { return sns_response.EncodeXml(s) }
func (s *ListTopicsResponse) HttpCode() int       { return http.StatusOK }

func ListTopics(tm *tmgr.TopicManager, snsQuery *sns_query.SNSQuery) sns_response.SNSResponse {

	arnList, _ := tm.ListTopics(0)

	arns := make([]TopicArn, len(arnList))
	for i, topicArn := range arnList {
		arns[i] = TopicArn{Arn: topicArn}
	}

	return &ListTopicsResponse{
		XMLName:   xml.Name{snsdefs.XMLSpace, "ListTopicsResponse"},
		TopicArns: arns,
		RequestId: "reqId",
	}
}
