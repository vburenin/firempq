package create_topic

import (
	"encoding/xml"
	"net/http"

	"github.com/vburenin/firempq/server/snsproto/sns_query"
	"github.com/vburenin/firempq/server/snsproto/sns_response"
	"github.com/vburenin/firempq/server/snsproto/snsdefs"
	"github.com/vburenin/firempq/server/snsproto/snserr"
	"github.com/vburenin/firempq/server/snsproto/tmgr"
	"github.com/vburenin/firempq/server/snsproto/validation"
)

type CreateTopicResponse struct {
	XMLName   xml.Name `xml:"http://sns.amazonaws.com/doc/2010-03-31/ CreateTopicResponse"`
	TopicArn  string   `xml:"CreateTopicResult>TopicArn"`
	RequestId string   `xml:"ResponseMetadata>RequestId"`
}

func (s *CreateTopicResponse) XmlDocument() string { return sns_response.EncodeXml(s) }
func (s *CreateTopicResponse) HttpCode() int       { return http.StatusOK }

func CreateTopic(tm *tmgr.TopicManager, snsQuery *sns_query.SNSQuery) sns_response.SNSResponse {
	topicName := ""
	sns_query.ParseParams(snsQuery, func(k, v string) {
		if k == "Name" {
			topicName = v
		}
	})

	if !validation.ValidateTopicName(topicName) {
		return snserr.InvalidParameterError("Invalid parameter: Topic Name")
	}

	arn := tm.CreateTopic(topicName)

	return &CreateTopicResponse{
		XMLName: xml.Name{
			Space: snsdefs.XMLSpace,
			Local: "CreateTopicResponse",
		},
		TopicArn:  arn,
		RequestId: "reqId",
	}
}
