package get_queue_attributes

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
	"github.com/vburenin/firempq/server/sqsproto/urlutils"
)

type QAttr struct {
	Name  string      `xml:"Name"`
	Value interface{} `xml:"Value"`
}

type GetQueueAttributesResponse struct {
	XMLName    xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ GetQueueAttributesResponse"`
	Attributes []*QAttr `xml:"GetQueueAttributesResult>Attribute,omitempty"`
	RequestId  string   `xml:"ResponseMetadata>RequestId"`
}

func (r *GetQueueAttributesResponse) HttpCode() int { return http.StatusOK }
func (r *GetQueueAttributesResponse) XmlDocument() string {
	return sqs_response.EncodeXml(r)
}
func (r *GetQueueAttributesResponse) BatchResult(docId string) interface{} { return nil }

const (
	AttrAll                                   = "All"
	AttrApproximateNumberOfMessages           = "ApproximateNumberOfMessages"
	AttrApproximateNumberOfMessagesNotVisible = "ApproximateNumberOfMessagesNotVisible"
	AttrApproximateNumberOfMessagesDelayed    = "ApproximateNumberOfMessagesDelayed"

	AttrVisibilityTimeout     = "VisibilityTimeout"
	AttrDelaySeconds          = "DelaySeconds"
	AttrCreatedTimestamp      = "CreatedTimestamp"
	AttrLastModifiedTimestamp = "LastModifiedTimestamp"

	AttrMaximumMessageSize            = "MaximumMessageSize"
	AttrMessageRetentionPeriod        = "MessageRetentionPeriod"
	AttrReceiveMessageWaitTimeSeconds = "ReceiveMessageWaitTimeSeconds"
	AttrQueueArn                      = "QueueArn"
	AttrRedrivePolicy                 = "RedrivePolicy"

	// Not used attribute.
	// AttrPolicy                                = "Policy"
)

func makeQueueArn(name string) string {
	return "arn:aws:sqs:us-west-2:123456789:" + name
}

func GetQueueAttributes(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	paramsLen := len(sqsQuery.ParamsList) - 1

	pqCfg := pq.Config()
	pqDesc := pq.Description()

	resp := &GetQueueAttributesResponse{
		RequestId: "reqId",
	}

	for i := 0; i < paramsLen; i += 2 {
		allAttr := false
		if strings.HasPrefix(sqsQuery.ParamsList[i], "AttributeName") {
			attrName := sqsQuery.ParamsList[i+1]
			if attrName == AttrAll {
				allAttr = true
				resp.Attributes = make([]*QAttr, 0, 13) // 13 total attributes
			}

			if allAttr || attrName == AttrApproximateNumberOfMessages {
				resp.Attributes = append(resp.Attributes, &QAttr{
					Name:  AttrApproximateNumberOfMessages,
					Value: pq.AvailableMessages(),
				})
			}

			if allAttr || attrName == AttrApproximateNumberOfMessagesNotVisible {
				resp.Attributes = append(resp.Attributes, &QAttr{
					Name:  AttrApproximateNumberOfMessagesNotVisible,
					Value: pq.LockedCount(),
				})
			}

			if allAttr || attrName == AttrVisibilityTimeout {
				resp.Attributes = append(resp.Attributes, &QAttr{
					Name:  AttrVisibilityTimeout,
					Value: pqCfg.PopLockTimeout / 1000,
				})
			}

			if allAttr || attrName == AttrCreatedTimestamp {
				resp.Attributes = append(resp.Attributes, &QAttr{
					Name:  AttrCreatedTimestamp,
					Value: pqDesc.CreateTs / 1000,
				})
			}

			if allAttr || attrName == AttrLastModifiedTimestamp {
				resp.Attributes = append(resp.Attributes, &QAttr{
					Name:  AttrLastModifiedTimestamp,
					Value: pqCfg.LastUpdateTs / 1000,
				})
			}

			if allAttr || attrName == AttrMaximumMessageSize {
				resp.Attributes = append(resp.Attributes, &QAttr{
					Name:  AttrMaximumMessageSize,
					Value: pqCfg.MaxMsgSize,
				})
			}

			if allAttr || attrName == AttrMessageRetentionPeriod {
				resp.Attributes = append(resp.Attributes, &QAttr{
					Name:  AttrMessageRetentionPeriod,
					Value: pqCfg.MsgTtl / 1000,
				})
			}

			if allAttr || attrName == AttrQueueArn {
				resp.Attributes = append(resp.Attributes, &QAttr{
					Name:  AttrQueueArn,
					Value: makeQueueArn(pqDesc.Name),
				})
			}

			if allAttr || attrName == AttrApproximateNumberOfMessagesDelayed {
				resp.Attributes = append(resp.Attributes, &QAttr{
					Name:  AttrApproximateNumberOfMessagesDelayed,
					Value: pq.DelayedCount(),
				})
			}

			if allAttr || attrName == AttrDelaySeconds {
				resp.Attributes = append(resp.Attributes, &QAttr{
					Name:  AttrDelaySeconds,
					Value: pqCfg.DeliveryDelay / 1000,
				})
			}

			if allAttr || attrName == AttrReceiveMessageWaitTimeSeconds {
				resp.Attributes = append(resp.Attributes, &QAttr{
					Name:  AttrReceiveMessageWaitTimeSeconds,
					Value: pqCfg.PopWaitTimeout / 1000,
				})
			}

			if allAttr || attrName == AttrRedrivePolicy {
				if pqCfg.PopLimitQueueName != "" {
					resp.Attributes = append(resp.Attributes, &QAttr{
						Name: AttrRedrivePolicy,
						Value: fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":%d}`,
							makeQueueArn(pqCfg.PopLimitQueueName), pqCfg.PopCountLimit),
					})
				}
			}

			if allAttr {
				break
			}
		}
	}
	return resp
}
