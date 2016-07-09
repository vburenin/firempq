package sns_query

import (
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

type SNSQuery struct {
	Host             string
	SenderId         string
	Action           string
	TopicArn         string
	ParamsList       []string
	Timestamp        string
	AWSAccessKeyId   string
	SignatureVersion string
	SignatureMethod  string
	Signature        string
}

type CustomParamHandler func(key, value string)

var maxFormSize = int64(30 * 1024 * 1024)
var ErrTooLargePost = errors.New("http: POST too large")

func ParseParams(snsQuery *SNSQuery, ch CustomParamHandler) {
	pl := snsQuery.ParamsList
	for i := 0; i < len(pl)-1; i++ {
		ch(pl[i], pl[i+1])
	}
}

func ParseSNSQuery(req *http.Request) (*SNSQuery, error) {
	query, err := getQueryString(req)
	if err != nil {
		return nil, err
	}

	var urlProto string
	if req.TLS == nil {
		urlProto = "http://"
	} else {
		urlProto = "https://"
	}

	snsQuery := &SNSQuery{
		Host: urlProto + req.Host,
	}

	for query != "" {
		key := query
		if i := strings.IndexAny(key, "&;"); i >= 0 {
			key, query = key[:i], key[i+1:]
		} else {
			query = ""
		}
		if key == "" {
			continue
		}
		value := ""
		if i := strings.Index(key, "="); i >= 0 {
			key, value = key[:i], key[i+1:]
		}
		key, err1 := url.QueryUnescape(key)
		if err1 != nil {
			if err == nil {
				err = err1
			}
			continue
		}
		value, err1 = url.QueryUnescape(value)
		if err1 != nil {
			if err == nil {
				err = err1
			}
			continue
		}
		switch key {
		case "Action":
			snsQuery.Action = value
		case "TopicArn":
			snsQuery.TopicArn = value
		case "SignatureVersion":
			snsQuery.SignatureVersion = value
		case "SignatureMethod":
			snsQuery.SignatureMethod = value
		case "Timestamp":
			snsQuery.Timestamp = value
		case "AWSAccessKeyId":
			snsQuery.AWSAccessKeyId = value
		case "Signature":
			snsQuery.Signature = value
		default:
			snsQuery.ParamsList = append(snsQuery.ParamsList, key, value)
		}
	}
	snsQuery.SenderId = req.RemoteAddr
	return snsQuery, err
}

func getQueryString(req *http.Request) (string, error) {
	if req.Method == "POST" {
		b, e := ioutil.ReadAll(io.LimitReader(req.Body, maxFormSize+1))
		if e != nil {
			return "", e
		}
		if int64(len(b)) > maxFormSize {
			return "", ErrTooLargePost
		}
		return string(b), nil
	}
	if req.Method == "GET" {
		return req.URL.RawQuery, nil
	}
	return "", errors.New("Unsupported method " + req.Method)
}
