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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
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

func (self *SQSRequestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var resp sqs_response.SQSResponse = nil
	sqsQuery, err := urlutils.ParseSQSQuery(r)
	if err != nil {
		sqserr.ServiceDeniedError()
	}
	if strings.HasPrefix(r.URL.Path, "/queue/") {
		sqsQuery.QueueName = strings.SplitN(r.URL.Path, "/queue/", 2)[1]
		svc, ok := self.ServiceManager.GetService(sqsQuery.QueueName)
		if !ok {
			resp = sqserr.InvalidActionError("No queue")
		}
		if svc.GetTypeName() != common.STYPE_PRIORITY_QUEUE {
			resp = sqserr.QueueDoesNotExist()
		} else {
			fmt.Println(r.Method)
			fmt.Println(r.URL.RawQuery)
			data, _ := ioutil.ReadAll(r.Body)
			fmt.Println(string(data))
			pq, _ := svc.(*pqueue.PQueue)
			resp = self.handleQueueActions(pq, sqsQuery)
		}

	} else if r.URL.Path == "/" {
		resp = self.handleManageActions(sqsQuery)
	} else {

	}
	if resp == nil {
		return
	}
	log.Info(resp.XmlDocument())
	w.WriteHeader(resp.HttpCode())
	io.WriteString(w, resp.XmlDocument())
	io.WriteString(w, "\n")
}
