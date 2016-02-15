package send_message

import (
	"encoding/xml"
	"fmt"
	"strconv"

	"crypto/md5"
	"firempq/common"
	"firempq/conf"
	"firempq/server/sqsproto/sqs_response"
	"firempq/server/sqsproto/sqsencoding"
	"firempq/server/sqsproto/sqserr"
	"firempq/server/sqsproto/urlutils"
	"firempq/server/sqsproto/validation"
	"firempq/services/pqueue"
	"firempq/services/pqueue/pqmsg"
	"firempq/utils"
	"net/http"
	"sort"
	"strings"
)

type SendMessageResponse struct {
	XMLName                xml.Name `xml:"http://queue.amazonaws.com/doc/2012-11-05/ SendMessageResponse"`
	MessageId              string   `xml:"SendMessageResult>MessageId"`
	MD5OfMessageBody       string   `xml:"SendMessageResult>MD5OfMessageBody"`
	MD5OfMessageAttributes string   `xml:"SendMessageResult>MD5OfMessageAttributes,omitempty"`
	RequestId              string   `xml:"ResponseMetadata>RequestId"`
}

func (self *SendMessageResponse) HttpCode() int       { return http.StatusOK }
func (self *SendMessageResponse) XmlDocument() string { return sqsencoding.EncodeXmlDocument(self) }

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
		self.BinaryValue = value
	}
	return nil
}

func EncodeAttrTo(name string, data *pqmsg.MsgAttr, b []byte) []byte {
	nLen := len(name)
	b = append(b, byte(nLen>>24), byte(nLen>>16), byte(nLen>>8), byte(nLen))
	b = append(b, utils.UnsafeStringToBytes(name)...)

	tLen := len(data.TypeName)
	b = append(b, byte(tLen>>24), byte(tLen>>16), byte(tLen>>8), byte(tLen))
	b = append(b, utils.UnsafeStringToBytes(data.TypeName)...)

	if strings.HasPrefix(data.TypeName, "String") || strings.HasPrefix(data.TypeName, "Number") {
		b = append(b, 1)
	} else if strings.HasPrefix(data.TypeName, "Binary") {
		b = append(b, 2)
	}
	valLen := len(data.Value)
	b = append(b, byte(valLen>>24), byte(valLen>>16), byte(valLen>>8), byte(valLen))
	b = append(b, utils.UnsafeStringToBytes(data.Value)...)
	return b
}

// TODO(vburenin): If Binary content is present MD5 checksum is wrong for some reason.
// Amazon doc: http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSMessageAttributes.html
// It looks like it is SQS bug on AWS side.
func calcAttrMd5(attrMap map[string]*pqmsg.MsgAttr) string {
	if len(attrMap) == 0 {
		return ""
	}

	keys := make([]string, 0, len(attrMap))
	dataLen := 0
	for k, v := range attrMap {
		keys = append(keys, k)
		dataLen += len(k) + len(v.TypeName) + len(v.Value) + 13 // 4(name) + 4(type) + 4(val) + 1(wireType) == 13
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

var IdGen = common.NewIdGen()

func SendMessage(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	var err *sqserr.SQSError
	out := &MessageParams{
		DelaySeconds: -1,
		MessageBody:  "",
	}

	attrs, err := urlutils.ParseNNotationAttr("MessageAttribute.", sqsQuery.ParamsList, out.Parse, NewReqQueueAttr)

	if err != nil {
		return err
	}

	attrsLen := len(attrs)
	outAttrs := make(map[string]*pqmsg.MsgAttr)

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
			outAttrs[reqMsgAttr.Name] = &pqmsg.MsgAttr{
				TypeName: reqMsgAttr.DataType,
				Value:    reqMsgAttr.BinaryValue,
			}
		} else {
			outAttrs[reqMsgAttr.Name] = &pqmsg.MsgAttr{
				TypeName: reqMsgAttr.DataType,
				Value:    reqMsgAttr.StringValue,
			}
		}
	}

	msgId := IdGen.GenRandId()
	if out.DelaySeconds < 0 {
		out.DelaySeconds = pq.Config().DeliveryDelay
	} else if out.DelaySeconds > conf.CFG_PQ.MaxDeliveryDelay {
		return sqserr.InvalidParameterValueError(
			"Delay secods must be between 0 and %d", conf.CFG_PQ.MaxDeliveryDelay/1000)
	}

	resp := pq.Push(msgId, out.MessageBody, pq.Config().MsgTtl, out.DelaySeconds, 1, outAttrs)
	if resp.IsError() {
		e, _ := resp.(error)
		return sqserr.InvalidParameterValueError(e.Error())
	}

	bodyMd5str := fmt.Sprintf("%x", md5.Sum(utils.UnsafeStringToBytes(out.MessageBody)))
	attrMd5 := calcAttrMd5(outAttrs)

	return &SendMessageResponse{
		MessageId:              msgId,
		MD5OfMessageBody:       bodyMd5str,
		MD5OfMessageAttributes: attrMd5,
		RequestId:              "req",
	}
}
