package resp

import (
	"bytes"
	"io"
	"strconv"

	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/enc"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/mpqerr"
)

func TranslateError(err error) apis.IResponse {
	if err == nil {
		return OK_RESPONSE
	}
	if resp, ok := err.(apis.IResponse); ok {
		return resp
	} else {
		log.Error(err.Error())
		return mpqerr.ERR_UNKNOWN_ERROR
	}
}

// StrResponse is a simple string response used to return some quick responses for commands like ping, etc.
type StrResponse struct {
	str string
}

func NewStrResponse(str string) *StrResponse {
	return &StrResponse{str: "+" + str}
}

func NewAsyncAccept(str string) *StrResponse {
	return &StrResponse{str: "+A " + str}
}

func (r *StrResponse) GetResponse() string {
	return r.str
}

func (r *StrResponse) WriteResponse(buff io.Writer) error {
	_, err := buff.Write(enc.UnsafeStringToBytes(r.GetResponse()))
	return err
}

func (r *StrResponse) IsError() bool {
	return false
}

// IntResponse is a simple integer response.
type IntResponse struct {
	Value int64
}

func NewIntResponse(val int64) *IntResponse {
	return &IntResponse{Value: val}
}

func (r *IntResponse) GetResponse() string {
	return "+DATA :" + strconv.FormatInt(r.Value, 10)
}

var intDataPrefix = []byte("+DATA :")

func (r *IntResponse) WriteResponse(buff io.Writer) error {
	_, err := buff.Write(intDataPrefix)
	if err == nil {
		_, err = buff.Write(enc.UnsafeStringToBytes(strconv.FormatInt(r.Value, 10)))
	}
	return err
}

func (r *IntResponse) IsError() bool {
	return false
}

// StrArrayResponse a response containing array of strings.
type StrArrayResponse struct {
	val    []string
	header string
}

func NewStrArrayResponse(header string, val []string) *StrArrayResponse {
	return &StrArrayResponse{
		val:    val,
		header: header,
	}
}

func (r *StrArrayResponse) IsError() bool {
	return false
}

func (r *StrArrayResponse) genResponse() []byte {
	var buffer bytes.Buffer
	buffer.WriteString(r.header)
	buffer.WriteString(" *")
	buffer.WriteString(strconv.Itoa(len(r.val)))
	for _, v := range r.val {
		buffer.WriteString(" ")
		buffer.WriteString(v)
	}
	return buffer.Bytes()
}

func (r *StrArrayResponse) GetResponse() string {
	return enc.UnsafeBytesToString(r.genResponse())
}

func (r *StrArrayResponse) WriteResponse(buff io.Writer) error {
	_, err := buff.Write(r.genResponse())
	return err
}

type AsyncResponse struct {
	asyncHeader string
	resp        apis.IResponse
}

func NewAsyncResponse(asyncId string, resp apis.IResponse) apis.IResponse {
	return &AsyncResponse{
		asyncHeader: "+ASYNC " + asyncId + " ",
		resp:        resp}
}

func (r *AsyncResponse) GetResponse() string {
	return r.asyncHeader + r.resp.GetResponse()
}

func (r *AsyncResponse) IsError() bool {
	return r.resp.IsError()
}

func (r *AsyncResponse) WriteResponse(buff io.Writer) error {

	if _, err := buff.Write(enc.UnsafeStringToBytes(r.asyncHeader)); err != nil {
		return err
	}
	return r.resp.WriteResponse(buff)
}

var RESP_PONG apis.IResponse = NewStrResponse("PONG")
var OK_RESPONSE = NewStrResponse("OK")
