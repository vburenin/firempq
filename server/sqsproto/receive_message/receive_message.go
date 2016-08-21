package receive_message

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/enc"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/mpqproto/resp"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
	"github.com/vburenin/firempq/server/sqsproto/sqserr"
	"github.com/vburenin/firempq/server/sqsproto/sqsmsg"
	"github.com/vburenin/firempq/server/sqsproto/urlutils"
	"github.com/vburenin/firempq/utils"
)

const (
	AttrAll                              = "All"
	AttrSenderId                         = "SenderId"
	AttrApproximateFirstReceiveTimestamp = "ApproximateFirstReceiveTimestamp"
	AttrApproximateReceiveCount          = "ApproximateReceiveCount"
	AttrSentTimestamp                    = "SentTimestamp"
)

type SysAttribute struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

type MessageAttribute struct {
	Name        string `xml:"Name"`
	Type        string `xml:"Value>DataType"`
	BinaryValue string `xml:"Value>BinaryValue,omitempty"`
	StringValue string `xml:"Value>StringValue,omitempty"`
}

type MessageResponse struct {
	MD5OfMessageBody       string              `xml:"MD5OfBody"`
	Body                   string              `xml:"Body"`
	ReceiptHandle          string              `xml:"ReceiptHandle"`
	Attributes             []*SysAttribute     `xml:"Attribute,omitempty"`
	MessageAttributes      []*MessageAttribute `xml:"MessageAttribute,omitempty"`
	MessageId              string              `xml:"MessageId"`
	MD5OfMessageAttributes string              `xml:"MD5OfMessageAttributes,omitempty"`
}

type ReceiveMessageResponse struct {
	XMLName   xml.Name           `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ReceiveMessageResponse"`
	Message   []*MessageResponse `xml:"ReceiveMessageResult>Message"`
	RequestId string             `xml:"ResponseMetadata>RequestId"`
}

func (self *ReceiveMessageResponse) HttpCode() int                        { return http.StatusOK }
func (self *ReceiveMessageResponse) XmlDocument() string                  { return sqs_response.EncodeXml(self) }
func (self *ReceiveMessageResponse) BatchResult(docId string) interface{} { return nil }

type ReceiveMessageOptions struct {
	WaitTimeSeconds      int64
	VisibilityTimeout    int64
	MaxNumberOfMessages  int64
	Attributes           []string
	MessageAttributes    []string
	AllMessageAttributes bool
	AllSysAttributes     bool
}

func (self *ReceiveMessageOptions) Parse(paramName, value string) *sqserr.SQSError {
	var err error
	switch paramName {
	case "VisibilityTimeout":
		self.VisibilityTimeout, err = strconv.ParseInt(value, 10, 0)
		if err != nil {
			return sqserr.MalformedInputError("VisibilityTimeout must be a positive integer value")
		}
		self.VisibilityTimeout *= 1000
		return nil
	case "WaitTimeSeconds":
		self.WaitTimeSeconds, err = strconv.ParseInt(value, 10, 0)
		if err != nil {
			return sqserr.MalformedInputError("WaitTimeSeconds must be a positive integer value")
		}
		self.WaitTimeSeconds *= 1000
		return nil
	case "MaxNumberOfMessages":
		self.MaxNumberOfMessages, err = strconv.ParseInt(value, 10, 0)
		if err != nil {
			return sqserr.MalformedInputError("MaxNumberOfMessages must be a positive integer value")
		}
		return nil
	}

	if strings.HasPrefix(paramName, "AttributeName") {
		if value == AttrAll {
			self.AllSysAttributes = true
		} else {
			self.Attributes = append(self.Attributes, value)
		}
	} else if strings.HasPrefix(paramName, "MessageAttributeName") {
		if value == AttrAll {
			self.AllMessageAttributes = true
		} else {
			self.MessageAttributes = append(self.MessageAttributes, value)
		}
	}

	return nil
}

func MakeMessageAttr(name string, sqsAttr *sqsmsg.UserAttribute) *MessageAttribute {
	if strings.HasPrefix(sqsAttr.Type, "Binary") {
		encodedBin := make([]byte, base64.RawStdEncoding.EncodedLen(len(sqsAttr.Value)))
		base64.RawStdEncoding.Encode(encodedBin, enc.UnsafeStringToBytes(sqsAttr.Value))
		return &MessageAttribute{
			Name:        name,
			Type:        sqsAttr.Type,
			BinaryValue: enc.UnsafeBytesToString(encodedBin),
		}
	} else {
		return &MessageAttribute{
			Name:        name,
			Type:        sqsAttr.Type,
			StringValue: sqsAttr.Value,
		}
	}
}

func MakeMessageResponse(iMsg apis.IResponseItem, opts *ReceiveMessageOptions,
	sqsQuery *urlutils.SQSQuery) *MessageResponse {
	msg, ok := iMsg.(*pqueue.MsgResponseItem)
	if !ok {
		return nil
	}
	sqsMsg := &sqsmsg.SQSMessagePayload{}
	payload := msg.Payload()
	if err := sqsMsg.Unmarshal([]byte(payload)); err != nil {
		// Recovering from error. Non SQS messages will be filled with bulk info.
		sqsMsg.Payload = string(msg.Payload())
		sqsMsg.SenderId = "unknown"
		sqsMsg.SentTimestamp = strconv.FormatInt(utils.Uts(), 10)
		sqsMsg.MD5OfMessageAttributes = fmt.Sprintf("%x", md5.Sum(nil))
		sqsMsg.MD5OfMessageBody = fmt.Sprintf("%x", md5.Sum(enc.UnsafeStringToBytes(sqsMsg.Payload)))
	}

	msgMeta := msg.GetMeta()

	output := &MessageResponse{
		MD5OfMessageAttributes: sqsMsg.MD5OfMessageAttributes,
		MD5OfMessageBody:       sqsMsg.MD5OfMessageBody,
		ReceiptHandle:          msg.Receipt(),
		MessageId:              msg.ID(),
		Body:                   sqsMsg.Payload,
	}

	if opts.AllSysAttributes {
		output.Attributes = append(output.Attributes, &SysAttribute{
			Name: AttrSenderId, Value: sqsMsg.SenderId,
		})
		output.Attributes = append(output.Attributes, &SysAttribute{
			Name: AttrSentTimestamp, Value: sqsMsg.SentTimestamp,
		})
		output.Attributes = append(output.Attributes, &SysAttribute{
			Name: AttrApproximateReceiveCount, Value: strconv.FormatInt(msgMeta.PopCount, 10),
		})
		output.Attributes = append(output.Attributes, &SysAttribute{
			Name: AttrApproximateFirstReceiveTimestamp, Value: strconv.FormatInt(utils.Uts(), 10),
		})
	} else {
		for _, k := range opts.Attributes {
			switch k {
			case AttrSenderId:
				output.Attributes = append(output.Attributes, &SysAttribute{
					Name: AttrSenderId, Value: sqsMsg.SenderId,
				})
			case AttrSentTimestamp:
				output.Attributes = append(output.Attributes, &SysAttribute{
					Name: AttrSentTimestamp, Value: sqsMsg.SentTimestamp,
				})
			case AttrApproximateReceiveCount:
				output.Attributes = append(output.Attributes, &SysAttribute{
					Name: AttrApproximateReceiveCount, Value: strconv.FormatInt(msgMeta.PopCount, 10),
				})
			case AttrApproximateFirstReceiveTimestamp:
				output.Attributes = append(output.Attributes, &SysAttribute{
					Name: AttrApproximateFirstReceiveTimestamp, Value: strconv.FormatInt(utils.Uts(), 10),
				})
			}
		}
	}

	if opts.AllMessageAttributes {
		for attrName, attrData := range sqsMsg.UserAttributes {
			output.MessageAttributes = append(
				output.MessageAttributes, MakeMessageAttr(attrName, attrData))
		}
	} else {
		for _, attrName := range opts.MessageAttributes {
			if v, ok := sqsMsg.UserAttributes[attrName]; ok {
				output.MessageAttributes = append(
					output.MessageAttributes, MakeMessageAttr(attrName, v))
			}
		}
	}

	if !ok {
		log.Error("Failed to cast response item")
		return nil
	}
	return output
}

func ReceiveMessage(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	pqCfg := pq.Config()
	opts := &ReceiveMessageOptions{
		WaitTimeSeconds:     pqCfg.PopWaitTimeout,
		VisibilityTimeout:   pqCfg.PopLockTimeout,
		MaxNumberOfMessages: 1,
		Attributes:          nil,
		MessageAttributes:   nil,
	}

	paramsLen := len(sqsQuery.ParamsList) - 1
	for i := 0; i < paramsLen; i += 2 {
		err := opts.Parse(sqsQuery.ParamsList[i], sqsQuery.ParamsList[i+1])
		if err != nil {
			return err
		}
	}
	// All messages received from SQS must be locked.
	res := pq.Pop(opts.VisibilityTimeout, opts.WaitTimeSeconds, opts.MaxNumberOfMessages, true)
	if res.IsError() {
		e, _ := res.(error)
		return sqserr.InvalidParameterValueError(e.Error())
	}

	m, _ := res.(*resp.MessagesResponse)
	items := m.GetItems()

	output := &ReceiveMessageResponse{}
	for _, item := range items {
		if msgResp := MakeMessageResponse(item, opts, sqsQuery); msgResp != nil {
			output.Message = append(output.Message, msgResp)
		}
	}
	return output
}
