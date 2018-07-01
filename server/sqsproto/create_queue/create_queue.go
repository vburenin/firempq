package create_queue

import (
	"encoding/xml"
	"net/http"
	"strconv"

	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/fctx"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/qmgr"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
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

func (r *CreateQueueResponse) XmlDocument() string                  { return sqs_response.EncodeXml(r) }
func (r *CreateQueueResponse) HttpCode() int                        { return http.StatusOK }
func (r *CreateQueueResponse) BatchResult(docId string) interface{} { return nil }

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

func (r *QueueAttributes) HandleAttribute(paramName, value string) *sqserr.SQSError {
	var err error
	switch paramName {
	case AttrVisibilityTimeout:
		r.VisibilityTimeout, err = strconv.ParseInt(value, 10, 0)
		r.VisibilityTimeout *= 1000
	case AttrDelaySeconds:
		r.DelaySeconds, err = strconv.ParseInt(value, 10, 0)
		r.DelaySeconds *= 1000
	case AttrMaximumMessageSize:
		r.MaximumMessageSize, err = strconv.ParseInt(value, 10, 0)
	case AttrMessageRetentionPeriod:
		r.MessageRetentionPeriod, err = strconv.ParseInt(value, 10, 0)
		r.MessageRetentionPeriod *= 1000
	case AttrReceiveMessageWaitTimeSeconds:
		r.ReceiveMessageWaitTimeSeconds, err = strconv.ParseInt(value, 10, 0)
		r.ReceiveMessageWaitTimeSeconds *= 1000
	default:
		return sqserr.InvalidAttributeNameError("Unknown Attribute " + paramName + ".")
	}
	if err != nil {
		return sqserr.MalformedInputError("Invalid value for the parameter " + paramName)
	}
	return nil
}

func (r *QueueAttributes) MakePQConfig() *conf.PQConfig {
	cfg := pqueue.DefaultPQConfig()
	if r.DelaySeconds >= 0 {
		cfg.DeliveryDelay = r.DelaySeconds
	}
	if r.MessageRetentionPeriod >= 0 {
		cfg.MsgTtl = r.MessageRetentionPeriod
	}
	if r.VisibilityTimeout >= 0 {
		cfg.PopLockTimeout = r.VisibilityTimeout
	}
	if r.RedrivePolicy >= 0 {
		cfg.PopCountLimit = r.RedrivePolicy
	}
	if r.ReceiveMessageWaitTimeSeconds >= 0 {
		cfg.PopWaitTimeout = r.ReceiveMessageWaitTimeSeconds
	}
	if r.MaximumMessageSize > 0 {
		cfg.MaxMsgSize = r.MaximumMessageSize
	}
	return cfg
}

type ReqQueueAttr struct {
	Name  string
	Value string
}

func NewReqQueueAttr() urlutils.ISubContainer { return &ReqQueueAttr{} }
func (r *ReqQueueAttr) Parse(paramName string, value string) *sqserr.SQSError {
	switch paramName {
	case "Name":
		r.Name = value
	case "Value":
		r.Value = value
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
	svcMgr *qmgr.QueueManager,
	attr *QueueAttributes,
	sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {

	queue := svcMgr.GetQueue(sqsQuery.QueueName)
	if queue != nil {
		pqConfig := queue.Config()
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

func CreateQueue(ctx *fctx.Context, svcMgr *qmgr.QueueManager, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	queueAttributes, parseErr := ParseCreateQueueAttributes(sqsQuery)
	if parseErr != nil {
		return parseErr
	}

	if errResp := CheckAvailableQueues(svcMgr, queueAttributes, sqsQuery); errResp != nil {
		return errResp
	}

	resp := svcMgr.CreateQueue(ctx, sqsQuery.QueueName, queueAttributes.MakePQConfig())
	if resp.IsError() {
		e, _ := resp.(error)
		return sqserr.ServerSideError(e.Error())
	}

	return &CreateQueueResponse{
		QueueUrl:  sqsQuery.Host + "/queue/" + sqsQuery.QueueName,
		RequestId: "1111-2222-3333",
	}
}
