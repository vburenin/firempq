package snsproto

import (
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/vburenin/firempq/pqueue"
	"github.com/vburenin/firempq/qmgr"
	"github.com/vburenin/firempq/server/sqsproto/sqs_response"
	"github.com/vburenin/firempq/server/sqsproto/sqserr"
	"github.com/vburenin/firempq/server/sqsproto/urlutils"
)

type SNSRequestHandler struct {
	ServiceManager *qmgr.ServiceManager
}

func (self *SNSRequestHandler) handleManageActions(sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	switch sqsQuery.Action {

	}
	return sqserr.InvalidActionError(sqsQuery.Action)
}

func (self *SNSRequestHandler) handleQueueActions(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	switch sqsQuery.Action {

	}
	return sqserr.InvalidActionError(sqsQuery.Action)
}

func (self *SNSRequestHandler) dispatchSQSQuery(r *http.Request) sqs_response.SQSResponse {
	var queuePath string

	sqsQuery, err := urlutils.ParseSQSQuery(r)
	if err != nil {
		return sqserr.ServiceDeniedError()
	}
	if sqsQuery.QueueUrl != "" {
		queueUrl, err := url.ParseRequestURI(sqsQuery.QueueUrl)
		if err != nil {
			return sqserr.ServiceDeniedError()
		}
		queuePath = queueUrl.Path
	} else {
		queuePath = r.URL.Path
	}
	if strings.HasPrefix(queuePath, "/queue/") {
		sqsQuery.QueueName = strings.SplitN(queuePath, "/queue/", 2)[1]
		svc, ok := self.ServiceManager.GetService(sqsQuery.QueueName)
		if !ok {
			return sqserr.QueueDoesNotExist()
		}
		pq, _ := svc.(*pqueue.PQueue)
		return self.handleQueueActions(pq, sqsQuery)

	} else if r.URL.Path == "/" {
		return self.handleManageActions(sqsQuery)
	}
	return sqserr.ServiceDeniedError()
}

func (self *SNSRequestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp := self.dispatchSQSQuery(r)
	if resp == nil {
		return
	}

	w.WriteHeader(resp.HttpCode())
	io.WriteString(w, resp.XmlDocument())
	io.WriteString(w, "\n")
}
