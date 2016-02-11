package create_queue

import (
	"encoding/xml"
	"strconv"
	"strings"

	"firempq/common"
	"firempq/conf"
	"firempq/log"
	"firempq/parsers"
	"firempq/server/sqsproto/sqs_response"
	"firempq/server/sqsproto/sqserr"
	"firempq/server/sqsproto/urlutils"
	"firempq/services"
	"firempq/services/pqueue"
	"net/http"
)

type QueueAttributes struct {
	// QueueName
	QueueName string
	// Max 900. Default 0.
	DelaySeconds int64
	// Max 262144. Default 262144.
	MaximumMessageSize int64
	// Min 1, Max 1209600, Default 345600.
	MessageRetentionPeriod int64
	// Min 0. Max 20.
	ReceiveMessageWaitTimeSeconds int64
	// Min 0, Max 43200. Default 30. Seconds.
	VisibilityTimeout int64
	// PopLimit. Default 0(Infinite). Min 1. Max 1000.
	RedrivePolicy int64
	// Queue to push died messages.
	DeadMessageQueue string
}

type CreateQueueResponse struct {
	XMLName   xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ CreateQueueResponse"`
	QueueUrl  string   `xml:"CreateQueueResult>QueueUrl"`
	RequestId string   `xml:"ResponseMetadata>RequestId"`
}

func (self *CreateQueueResponse) XmlDocument() string { return urlutils.EncodeXmlDocument(self) }
func (self *CreateQueueResponse) HttpCode() int       { return http.StatusOK }

func NewQueueAttributes() *QueueAttributes {
	return &QueueAttributes{
		QueueName:                     "",
		DelaySeconds:                  -1,
		MaximumMessageSize:            -1,
		MessageRetentionPeriod:        -1,
		ReceiveMessageWaitTimeSeconds: -1,
		VisibilityTimeout:             -1,
		RedrivePolicy:                 -1,
		DeadMessageQueue:              "",
	}
}

func (self *QueueAttributes) ToPqueueParams() (params []string) {
	if self.DelaySeconds >= 0 {
		params = append(params, pqueue.CPRM_DELIVERY_DELAY, strconv.FormatInt(self.DelaySeconds, 10))
	}
	if self.MessageRetentionPeriod >= 0 {
		params = append(params, pqueue.CPRM_MSG_TTL, strconv.FormatInt(self.MessageRetentionPeriod, 10))
	}
	if self.VisibilityTimeout >= 0 {
		params = append(params, pqueue.CPRM_LOCK_TIMEOUT, strconv.FormatInt(self.VisibilityTimeout, 10))

	}
	if self.RedrivePolicy >= 0 {
		params = append(params, pqueue.CPRM_POP_LIMIT, strconv.FormatInt(self.RedrivePolicy, 10))
	}
	if self.ReceiveMessageWaitTimeSeconds >= 0 {
		params = append(params, pqueue.CPRM_POP_WAIT, strconv.FormatInt(self.ReceiveMessageWaitTimeSeconds, 10))
	}
	return params
}

type RawAttr struct {
	Name  string
	Value string
}

// This is the approximate behavior of AWS SQS API. It seems a little weird in many cases, however, it works.
func ParseCreateQueueAttributes(sqsQuery *urlutils.SQSQuery) (*QueueAttributes, *sqserr.SQSError) {
	var err error
	out := NewQueueAttributes()

	if !parsers.ValidateServiceName(sqsQuery.QueueName) {
		return nil, sqserr.InvalidQueueNameError()
	}

	paramListLength := len(sqsQuery.ParamsList)

	var attrIdx int64 = 1
	var name string
	var value string
	for i := 0; i < paramListLength; i += 2 {
		attrName := sqsQuery.ParamsList[i]
		if strings.HasPrefix(attrName, "Attribute.") {
			blocks := strings.SplitN(attrName, ".", 3)
			if len(blocks) == 3 {
				// Incorrectly parsed value gives 0 back, which is not a valid in the sequence.
				// Presence of 0 will be used as a sign of error.
				v, _ := strconv.ParseInt(blocks[1], 10, 0)

				if v < 1 {
					return nil, sqserr.MalformedInputError("Start of list found where not expected")
				}

				if v != attrIdx {
					return nil, sqserr.MalformedInputError("End of list found where not expected")
				}

				if name == "" {
					name = sqsQuery.ParamsList[i+1]
				} else {
					value = sqsQuery.ParamsList[i+1]
					if value == "" {
						return nil, sqserr.EmptyValueError("No Value Found for " + name)
					}
					attrIdx += 1
					switch name {
					case AttrVisibilityTimeout:
						out.VisibilityTimeout, err = strconv.ParseInt(value, 10, 0)
						out.VisibilityTimeout *= 1000
					case AttrDelaySeconds:
						out.DelaySeconds, err = strconv.ParseInt(value, 10, 0)
						out.DelaySeconds *= 1000
					case AttrMaximumMessageSize:
						out.MaximumMessageSize, err = strconv.ParseInt(value, 10, 0)
						out.MaximumMessageSize *= 1000
					case AttrMessageRetentionPeriod:
						out.MessageRetentionPeriod, err = strconv.ParseInt(value, 10, 0)
						out.MessageRetentionPeriod *= 1000
					case AttrReceiveMessageWaitTimeSeconds:
						out.ReceiveMessageWaitTimeSeconds, err = strconv.ParseInt(value, 10, 0)
						out.ReceiveMessageWaitTimeSeconds *= 1000
					default:
						return nil, sqserr.InvalidAttributeNameError("Unknown Attribute " + name + ".")
					}
					if err != nil {
						return nil, sqserr.MalformedInputError("Invalid value for the parameter " + name)
					}
					name = ""
				}
			} else {
				return nil, sqserr.MalformedInputError("key: " + attrName + " is malformed")
			}
		}
	}
	return out, nil
}

const (
	errQueueExists                    = "A queue already exists with the same name and a different value for attribute "
	AttrVisibilityTimeout             = "VisibilityTimeout"
	AttrDelaySeconds                  = "DelaySeconds"
	AttrMaximumMessageSize            = "MaximumMessageSize"
	AttrMessageRetentionPeriod        = "MessageRetentionPeriod"
	AttrReceiveMessageWaitTimeSeconds = "ReceiveMessageWaitTimeSeconds"
)

func CheckAvailableQueues(
	svcMgr *services.ServiceManager,
	attr *QueueAttributes,
	sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {

	svc, ok := svcMgr.GetService(attr.QueueName)
	if ok {
		if svc.GetTypeName() != common.STYPE_PRIORITY_QUEUE {
			return sqserr.QueueAlreadyExistsError("Queue already exists for a different type of service")
		}
		pqConfig, ok := svc.ServiceConfig().(*conf.PQConfig)
		if !ok {
			log.Error("Unexpected config type from the found service!")
			return sqserr.ServerSideError("Queue config data error")
		}
		if attr.VisibilityTimeout >= 0 && attr.VisibilityTimeout != pqConfig.PopLockTimeout {
			return sqserr.QueueAlreadyExistsError(errQueueExists + AttrVisibilityTimeout)
		}
		if attr.DelaySeconds >= 0 && attr.DelaySeconds != pqConfig.DeliveryDelay {
			return sqserr.QueueAlreadyExistsError(errQueueExists + AttrDelaySeconds)
		}
		if attr.MaximumMessageSize >= 0 && attr.MaximumMessageSize != pqConfig.MaxMsgSize {
			return sqserr.QueueAlreadyExistsError(errQueueExists + AttrMaximumMessageSize)
		}
		if attr.MessageRetentionPeriod >= 0 && attr.MessageRetentionPeriod != pqConfig.MsgTtl {
			return sqserr.QueueAlreadyExistsError(errQueueExists + AttrMessageRetentionPeriod)
		}
		if attr.ReceiveMessageWaitTimeSeconds >= 0 && attr.ReceiveMessageWaitTimeSeconds != pqConfig.PopWaitTimeout {
			return sqserr.QueueAlreadyExistsError(errQueueExists + AttrReceiveMessageWaitTimeSeconds)
		}
		return &CreateQueueResponse{
			QueueUrl:  sqsQuery.Host + "/queue/" + sqsQuery.QueueName,
			RequestId: "1111-2222-3333",
		}
	}
	return nil
}

func CreateQueue(svcMgr *services.ServiceManager, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	queueAttrs, parseErr := ParseCreateQueueAttributes(sqsQuery)
	if parseErr != nil {
		return parseErr
	}

	if errResp := CheckAvailableQueues(svcMgr, queueAttrs, sqsQuery); errResp != nil {
		return errResp
	}

	resp := svcMgr.CreateService("pqueue", queueAttrs.QueueName, queueAttrs.ToPqueueParams())
	if resp.IsError() {
		e, _ := resp.(error)
		return sqserr.ServerSideError(e.Error())
	}
	return &CreateQueueResponse{
		QueueUrl:  sqsQuery.Host + "/queue/" + sqsQuery.QueueName,
		RequestId: "1111-2222-3333",
	}
}
