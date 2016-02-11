package urlutils

import (
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
	ParamsList      []string
}

func ParseSQSQuery(req *http.Request) (*SQSQuery, error) {
	query := req.URL.RawQuery
	var err error

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
		default:
			sqsQuery.ParamsList = append(sqsQuery.ParamsList, key, value)
		}
	}
	return sqsQuery, err
}
