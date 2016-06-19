package create_queue

import (
	"encoding/xml"
	"net/http"
	"strconv"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/qmgr"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
	"github.com/vburenin/firempq/server/sqsproto/sqsencoding"
	"github.com/vburenin/firempq/server/sqsproto/sqserr"
	"github.com/vburenin/firempq/server/sqsproto/urlutils"
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

func (self *CreateQueueResponse) XmlDocument() string                  { return sqsencoding.EncodeXmlDocument(self) }
func (self *CreateQueueResponse) HttpCode() int                        { return http.StatusOK }
func (self *CreateQueueResponse) BatchResult(docId string) interface{} { return nil }

func NewQueueAttributes() *QueueAttributes {
	return &QueueAttributes{
		DelaySeconds:                  -1,
		MaximumMessageSize:            -1,
		MessageRetentionPeriod:        -1,
		ReceiveMessageWaitTimeSeconds: -1,
		VisibilityTimeout:             -1,
		RedrivePolicy:                 -1,
		DeadMessageQueue:              "",
	}
}

func (self *QueueAttributes) HandleAttribute(paramName, value string) *sqserr.SQSError {
	var err error
	switch paramName {
	case AttrVisibilityTimeout:
		self.VisibilityTimeout, err = strconv.ParseInt(value, 10, 0)
		self.VisibilityTimeout *= 1000
	case AttrDelaySeconds:
		self.DelaySeconds, err = strconv.ParseInt(value, 10, 0)
		self.DelaySeconds *= 1000
	case AttrMaximumMessageSize:
		self.MaximumMessageSize, err = strconv.ParseInt(value, 10, 0)
		self.MaximumMessageSize *= 1000
	case AttrMessageRetentionPeriod:
		self.MessageRetentionPeriod, err = strconv.ParseInt(value, 10, 0)
		self.MessageRetentionPeriod *= 1000
	case AttrReceiveMessageWaitTimeSeconds:
		self.ReceiveMessageWaitTimeSeconds, err = strconv.ParseInt(value, 10, 0)
		self.ReceiveMessageWaitTimeSeconds *= 1000
	default:
		return sqserr.InvalidAttributeNameError("Unknown Attribute " + paramName + ".")
	}
	if err != nil {
		return sqserr.MalformedInputError("Invalid value for the parameter " + paramName)
	}
	return nil
}

func (self *QueueAttributes) MakePQConfig() *conf.PQConfig {
	cfg := pqueue.DefaultPQConfig()
	if self.DelaySeconds >= 0 {
		cfg.DeliveryDelay = self.DelaySeconds
	}
	if self.MessageRetentionPeriod >= 0 {
		cfg.MsgTtl = self.MessageRetentionPeriod
	}
	if self.VisibilityTimeout >= 0 {
		cfg.PopLockTimeout = self.VisibilityTimeout

	}
	if self.RedrivePolicy >= 0 {
		cfg.PopCountLimit = self.RedrivePolicy
	}
	if self.ReceiveMessageWaitTimeSeconds >= 0 {
		cfg.PopWaitTimeout = self.ReceiveMessageWaitTimeSeconds
	}
	return cfg
}

type ReqQueueAttr struct {
	Name  string
	Value string
}

func NewReqQueueAttr() urlutils.ISubContainer { return &ReqQueueAttr{} }
func (self *ReqQueueAttr) Parse(paramName string, value string) *sqserr.SQSError {
	switch paramName {
	case "Name":
		self.Name = value
	case "Value":
		self.Value = value
	default:
		return sqserr.InvalidAttributeNameError("Invalid attribute: " + paramName)
	}
	return nil
}

// This is the approximate behavior of AWS SQS API. It seems a little weird in many cases, however, it works.
func ParseCreateQueueAttributes(sqsQuery *urlutils.SQSQuery) (*QueueAttributes, *sqserr.SQSError) {
	var err *sqserr.SQSError
	out := NewQueueAttributes()
	attrs, err := urlutils.ParseNNotationAttr("Attribute.", sqsQuery.ParamsList, nil, NewReqQueueAttr)
	if err != nil {
		return nil, err
	}

	attrsSize := len(attrs)

	for i := 1; i <= attrsSize; i++ {
		v, ok := attrs[i]
		if !ok {
			return nil, sqserr.MalformedInputError("End of list found where not expected")
		}
		reqAttr, _ := v.(*ReqQueueAttr)
		if reqAttr.Name == "" {
			return nil, sqserr.MalformedInputError("End of list found where not expected")
		}
		if reqAttr.Value == "" {
			return nil, sqserr.EmptyValueError("No Value Found for " + reqAttr.Name)
		}
		err = out.HandleAttribute(reqAttr.Name, reqAttr.Value)
		if err != nil {
			return nil, err
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
	svcMgr *qmgr.ServiceManager,
	attr *QueueAttributes,
	sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {

	svc, ok := svcMgr.GetService(sqsQuery.QueueName)
	if ok {
		if svc.GetTypeName() != apis.STYPE_PRIORITY_QUEUE {
			return sqserr.QueueAlreadyExistsError("Queue already exists for a different type of service")
		}
		pq, _ := svc.(*pqueue.PQueue)
		pqConfig := pq.Config()
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

func CreateQueue(svcMgr *qmgr.ServiceManager, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	queueAttributes, parseErr := ParseCreateQueueAttributes(sqsQuery)
	if parseErr != nil {
		return parseErr
	}

	if errResp := CheckAvailableQueues(svcMgr, queueAttributes, sqsQuery); errResp != nil {
		return errResp
	}

	resp := svcMgr.CreatePQueue(sqsQuery.QueueName, queueAttributes.MakePQConfig())
	if resp.IsError() {
		e, _ := resp.(error)
		return sqserr.ServerSideError(e.Error())
	}

	return &CreateQueueResponse{
		QueueUrl:  sqsQuery.Host + "/queue/" + sqsQuery.QueueName,
		RequestId: "1111-2222-3333",
	}
}
