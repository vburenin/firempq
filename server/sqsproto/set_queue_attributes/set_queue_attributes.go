package set_queue_attributes

import (
	"encoding/xml"
	"net/http"
	"strconv"

	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
	"github.com/vburenin/firempq/server/sqsproto/sqsencoding"
	"github.com/vburenin/firempq/server/sqsproto/sqserr"
	"github.com/vburenin/firempq/server/sqsproto/urlutils"
)

type SetQueueAttributesResponse struct {
	XMLName   xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ SetQueueAttributesResponse"`
	RequestId string   `xml:"ResponseMetadata>RequestId"`
}

func (self *SetQueueAttributesResponse) HttpCode() int { return http.StatusOK }
func (self *SetQueueAttributesResponse) XmlDocument() string {
	return sqsencoding.EncodeXmlDocument(self)
}
func (self *SetQueueAttributesResponse) BatchResult(docId string) interface{} { return nil }

type Attribute struct {
	Name  string
	Value string
}

func NewAttribute() urlutils.ISubContainer {
	return &Attribute{}
}

func (a *Attribute) Parse(param string, value string) *sqserr.SQSError {
	a.Name = param
	a.Value = value
	return nil
}

const AttrErrText = "Invalid value for the parameter %s."

func SetQueueAttributes(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	attrs, err := urlutils.ParseNNotationAttr("MessageAttribute.", sqsQuery.ParamsList, nil, NewAttribute)
	if err != nil {
		return err
	}

	attrsLen := len(attrs)

	params := &pqueue.PQueueParams{}

	for i := 1; i < attrsLen; i++ {
		a, ok := attrs[i]
		if !ok {
			return sqserr.InvalidParameterValueError("The request must contain non-empty message attribute name.")
		}
		attr, _ := a.(*Attribute)
		err = nil
		switch attr.Name {
		case "DelaySeconds":
			if v, e := strconv.ParseInt(attr.Value, 10, 0); e == nil {
				v := v * 1000
				if v >= 0 && v <= conf.CFG_PQ.MaxDeliveryDelay {
					params.DeliveryDelay = &v
					continue
				}
			}
			return sqserr.InvalidAttributeValueError(AttrErrText, attr.Name)
		case "MaximumMessageSize":
			if v, e := strconv.ParseInt(attr.Value, 10, 0); e == nil {
				if v >= 1024 && v <= conf.CFG_PQ.MaxMessageSize {
					params.MaxMsgSize = &v
					continue
				}
			}
			return sqserr.InvalidAttributeValueError(AttrErrText, attr.Name)
		case "MessageRetentionPeriod":
			if v, e := strconv.ParseInt(attr.Value, 10, 0); e == nil {
				if v >= 60 && v <= 1209600 {
					v := v * 1000
					params.MsgTTL = &v
					continue
				}
			}
			return sqserr.InvalidAttributeValueError(AttrErrText, attr.Name)
		case "ReceiveMessageWaitTimeSeconds":
			if v, e := strconv.ParseInt(attr.Value, 10, 0); e == nil {
				v := v * 1000
				if v >= 60000 && v <= conf.CFG_PQ.MaxPopWaitTimeout {
					params.PopWaitTimeout = &v
					continue
				}
			}
			return sqserr.InvalidAttributeValueError(AttrErrText, attr.Name)
		case "VisibilityTimeout":
			if v, e := strconv.ParseInt(attr.Value, 10, 0); e == nil {
				v := v * 1000
				if v > 0 && v <= conf.CFG_PQ.MaxLockTimeout {
					params.PopLockTimeout = &v
					continue
				}
			}
			return sqserr.InvalidAttributeValueError(AttrErrText, attr.Name)
		// These parameters are just ignored.
		case "Policy":
		case "ApproximateNumberOfMessages":
		case "ApproximateNumberOfMessagesDelayed":
		case "ApproximateNumberOfMessagesNotVisible":
		case "CreatedTimestamp":
		case "LastModifiedTimestamp":
		case "QueueArn":
		// Any unexpected attribute will produce unexpected attribute error.
		default:
			return sqserr.InvalidAttributeNameError("Unknown Attribute: %s", attr.Name)
		}
	}

	pq.SetParams(params)
	return &SetQueueAttributesResponse{
		RequestId: "req",
	}
}
