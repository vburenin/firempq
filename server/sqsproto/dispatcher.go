package sqsproto

import (
	"firempq/common"
	"firempq/log"
	"firempq/server/sqsproto/create_queue"
	"firempq/server/sqsproto/get_queue_url"
	"firempq/server/sqsproto/send_message"
	"firempq/server/sqsproto/sqs_response"
	"firempq/server/sqsproto/sqserr"
	"firempq/server/sqsproto/urlutils"
	"firempq/services"
	"firempq/services/pqueue"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type SQSRequestHandler struct {
	ServiceManager *services.ServiceManager
}

func ParseQueueName(urlPath string) (string, error) {
	f := strings.SplitN(urlPath, "/", 2)
	if len(f) == 2 && f[0] == "pqueue" {
		return f[0], nil
	}
	return "", sqserr.MalformedInputError("Invalid URL Format")
}

func (self *SQSRequestHandler) handleManageActions(sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	switch sqsQuery.Action {
	case "CreateQueue":
		return create_queue.CreateQueue(self.ServiceManager, sqsQuery)
	case "GetQueueUrl":
		return get_queue_url.GetQueueUrl(self.ServiceManager, sqsQuery)
	case "ListQueues":
	case "ListDeadLetterSourceQueues":
	}
	return sqserr.InvalidActionError(sqsQuery.Action)
}

func (self *SQSRequestHandler) handleQueueActions(pq *pqueue.PQueue, sqsQuery *urlutils.SQSQuery) sqs_response.SQSResponse {
	switch sqsQuery.Action {
	case "CreateQueue":
	case "DeleteQueue":
	case "GetQueueUrl":
	case "SetQueueAttributes":
	case "GetQueueAttributes":
	case "ListQueues":
	case "PurgeQueue":
	case "AddPermission":
	case "RemovePermission":
	case "ChangeMessageVisiblity":
	case "ChangeMessageVisibilityBatch":
	case "DeleteMessage":
	case "DeleteMessageBatch":
	case "ListDeadLetterSourceQueues":
	case "ReceiveMessage":
	case "SendMessage":
		return send_message.SendMessage(pq, sqsQuery)
	case "SendMessageBatch":
	}
	return nil
}

func (self *SQSRequestHandler) dispatchSQSQuery(r *http.Request) sqs_response.SQSResponse {
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
			return sqserr.InvalidActionError("No queue")
		}
		if svc.GetTypeName() != common.STYPE_PRIORITY_QUEUE {
			return sqserr.QueueDoesNotExist()
		} else {
			pq, _ := svc.(*pqueue.PQueue)
			return self.handleQueueActions(pq, sqsQuery)
		}

	} else if r.URL.Path == "/" {
		return self.handleManageActions(sqsQuery)
	}
	return sqserr.ServiceDeniedError()
}

func (self *SQSRequestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp := self.dispatchSQSQuery(r)

	log.Info(resp.XmlDocument())
	w.WriteHeader(resp.HttpCode())
	io.WriteString(w, resp.XmlDocument())
	io.WriteString(w, "\n")
}
