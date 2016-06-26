package send_message

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/enc"
	"github.com/vburenin/firempq/idgen"
	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
	"github.com/vburenin/firempq/server/sqsproto/sqserr"
	"github.com/vburenin/firempq/server/sqsproto/sqsmsg"
	"github.com/vburenin/firempq/server/sqsproto/urlutils"
	"github.com/vburenin/firempq/server/sqsproto/validation"
	"github.com/vburenin/firempq/utils"
)

type SendMessageResponse struct {
	XMLName                xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ SendMessageResponse"`
	MessageId              string   `xml:"SendMessageResult>MessageId"`
	MD5OfMessageBody       string   `xml:"SendMessageResult>MD5OfMessageBody,omitempty"`
	MD5OfMessageAttributes string   `xml:"SendMessageResult>MD5OfMessageAttributes,omitempty"`
	RequestId              string   `xml:"ResponseMetadata>RequestId"`
}

type SendMessageBatchResult struct {
	Id                     string `xml:"Id,omitempty"`
	MessageId              string `xml:"MessageId,omitempty"`
	MD5OfMessageBody       string `xml:"MD5OfMessageBody,omitempty"`
	MD5OfMessageAttributes string `xml:"MD5OfMessageAttributes,omitempty"`
}

func (self *SendMessageResponse) HttpCode() int       { return http.StatusOK }
func (self *SendMessageResponse) XmlDocument() string { return sqs_response.EncodeXml(self) }
func (self *SendMessageResponse) BatchResult(docId string) interface{} {
	return &SendMessageBatchResult{
		Id:                     docId,
		MessageId:              self.MessageId,
		MD5OfMessageBody:       self.MD5OfMessageBody,
		MD5OfMessageAttributes: self.MD5OfMessageAttributes,
	}
}

type ReqMsgAttr struct {
	Name        string
	DataType    string
	StringValue string
	BinaryValue string
}

func NewReqQueueAttr() urlutils.ISubContainer { return &ReqMsgAttr{} }
func (self *ReqMsgAttr) Parse(paramName string, value string) *sqserr.SQSError {
	switch paramName {
	case "Name":
		self.Name = value
	case "Value.DataType":
		self.DataType = value
	case "Value.StringValue":
		self.StringValue = value
	case "Value.BinaryValue":
		binValue, err := base64.RawStdEncoding.DecodeString(value)
		if err != nil {
			return sqserr.InvalidParameterValueError("Invalid binary data: %s", err.Error())
		}
		self.BinaryValue = string(binValue)
	}
	return nil
}

func EncodeAttrTo(name string, data *sqsmsg.UserAttribute, b []byte) []byte {
	nLen := len(name)
	b = append(b, byte(nLen>>24), byte(nLen>>16), byte(nLen>>8), byte(nLen))
	b = append(b, enc.UnsafeStringToBytes(name)...)

	tLen := len(data.Type)
	b = append(b, byte(tLen>>24), byte(tLen>>16), byte(tLen>>8), byte(tLen))
	b = append(b, enc.UnsafeStringToBytes(data.Type)...)

	if strings.HasPrefix(data.Type, "String") || strings.HasPrefix(data.Type, "Number") {
		b = append(b, 1)
	} else if strings.HasPrefix(data.Type, "Binary") {
		b = append(b, 2)
	}
	valLen := len(data.Value)
	b = append(b, byte(valLen>>24), byte(valLen>>16), byte(valLen>>8), byte(valLen))
	b = append(b, enc.UnsafeStringToBytes(data.Value)...)
	return b
}

// Calculates MD5 for message attributes.
// Amazon doc: http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSMessageAttributes.html
// There is a caveat, all binary content must be base64 encoded when transferred over the wire.
func CalcAttrMd5(attrMap map[string]*sqsmsg.UserAttribute) string {
	if len(attrMap) == 0 {
		return ""
	}

	keys := make([]string, 0, len(attrMap))
	dataLen := 0
	for k, v := range attrMap {
		keys = append(keys, k)
		dataLen += len(k) + len(v.Type) + len(v.Value) + 13 // 4(name) + 4(type) + 4(val) + 1(wireType) == 13
	}
	sort.Strings(keys)

	md5buf := make([]byte, 0, dataLen)

	for _, k := range keys {
		md5buf = EncodeAttrTo(k, attrMap[k], md5buf)
	}
	return fmt.Sprintf("%x", md5.Sum(md5buf))
}

type MessageParams struct {
	DelaySeconds int64
	MessageBody  string
}

func (self *MessageParams) Parse(paramName, value string) *sqserr.SQSError {
	var err error
	switch paramName {
	case "DelaySeconds":
		self.DelaySeconds, err = strconv.ParseInt(value, 10, 0)
		self.DelaySeconds *= 1000
	case "MessageBody":
		self.MessageBody = value
	}

	if err != nil {
		return sqserr.MalformedInputError("Invalid value for the parameter " + paramName)
	}

	return nil
}

var IdGen = idgen.NewGen()

func PushAMessage(pq *pqueue.PQueue, senderId string, paramList []string) sqs_response.SQSResponse {
	out := &MessageParams{
		DelaySeconds: -1,
		MessageBody:  "",
	}
	attrs, err := urlutils.ParseNNotationAttr("MessageAttribute.", paramList, out.Parse, NewReqQueueAttr)
	if err != nil {
		return err
	}
	attrsLen := len(attrs)
	outAttrs := make(map[string]*sqsmsg.UserAttribute)

	for i := 1; i <= attrsLen; i++ {
		a, ok := attrs[i]
		if !ok {
			return sqserr.InvalidParameterValueError("The request must contain non-empty message attribute name.")
		}
		reqMsgAttr, _ := a.(*ReqMsgAttr)

		sqs_err := validation.ValidateMessageAttrName(reqMsgAttr.Name)
		if sqs_err != nil {
			return sqs_err
		}

		sqs_err = validation.ValidateMessageAttrName(reqMsgAttr.DataType)
		if sqs_err != nil {
			return sqs_err
		}

		if reqMsgAttr.BinaryValue != "" && reqMsgAttr.StringValue != "" {
			return sqserr.InvalidParameterValueError(
				"Message attribute name '%s' has multiple values.", reqMsgAttr.Name)
		}

		if _, ok := outAttrs[reqMsgAttr.Name]; ok {
			return sqserr.InvalidParameterValueError(
				"Message attribute name '%s' already exists.", reqMsgAttr.Name)
		}

		if strings.HasPrefix(reqMsgAttr.DataType, "Number") {
			if _, err := strconv.Atoi(reqMsgAttr.StringValue); err != nil {
				return sqserr.InvalidParameterValueError(
					"Could not cast message attribute '%s' value to number.", reqMsgAttr.Name)
			}
		}

		if reqMsgAttr.BinaryValue != "" {
			outAttrs[reqMsgAttr.Name] = &sqsmsg.UserAttribute{
				Type:  reqMsgAttr.DataType,
				Value: reqMsgAttr.BinaryValue,
			}
		} else {
			outAttrs[reqMsgAttr.Name] = &sqsmsg.UserAttribute{
				Type:  reqMsgAttr.DataType,
				Value: reqMsgAttr.StringValue,
			}
		}
	}

	msgId := IdGen.RandId()
	if out.DelaySeconds < 0 {
		out.DelaySeconds = pq.Config().DeliveryDelay
	} else if out.DelaySeconds > conf.CFG_PQ.MaxDeliveryDelay {
		return sqserr.InvalidParameterValueError(
			"Delay secods must be between 0 and %d", conf.CFG_PQ.MaxDeliveryDelay/1000)
	}
	bodyMd5str := fmt.Sprintf("%x", md5.Sum(enc.UnsafeStringToBytes(out.MessageBody)))
	attrMd5 := CalcAttrMd5(outAttrs)

	msgPayload := sqsmsg.SQSMessagePayload{
		UserAttributes:         outAttrs,
		MD5OfMessageBody:       bodyMd5str,
		MD5OfMessageAttributes: attrMd5,
		SenderId:               senderId,
		SentTimestamp:          strconv.FormatInt(utils.Uts(), 10),
		Payload:                out.MessageBody,
	}

	d, _ := msgPayload.Marshal()
	payload := enc.UnsafeBytesToString(d)

	resp := pq.Push(msgId, payload, pq.Config().MsgTtl, out.DelaySeconds, 1)
	if resp.IsError() {
		e, _ := resp.(error)
		return sqserr.InvalidParameterValueError(e.Error())
	}

	return &SendMessageResponse{
		MessageId:              msgId,
		MD5OfMessageBody:       bodyMd5str,
		MD5OfMessageAttributes: attrMd5,
		RequestId:              "req",
	}
}

func SendMessage(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	return PushAMessage(pq, sqsQuery.SenderId, sqsQuery.ParamsList)
}
