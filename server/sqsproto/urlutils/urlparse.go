package urlutils

import (
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

type SQSQuery struct {
	Action          string
	Host            string
	Version         string
	Expires         string
	QueueName       string
	QueueNamePrefix string
	QueueUrl        string
	ParamsList      []string
	SenderId        string
}

var maxFormSize = int64(30 * 1024 * 1024)
var ErrTooLargePost = errors.New("http: POST too large")

func ParseSQSQuery(req *http.Request) (*SQSQuery, error) {
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

	sqsQuery := &SQSQuery{
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
			sqsQuery.Action = value
		case "Version":
			sqsQuery.Version = value
		case "Expires":
			sqsQuery.Expires = value
		case "QueueName":
			sqsQuery.QueueName = value
		case "QueueNamePrefix":
			sqsQuery.QueueNamePrefix = value
		case "QueueUrl":
			sqsQuery.QueueUrl = value
		default:
			sqsQuery.ParamsList = append(sqsQuery.ParamsList, key, value)
		}
	}
	sqsQuery.SenderId = req.RemoteAddr
	return sqsQuery, err
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
