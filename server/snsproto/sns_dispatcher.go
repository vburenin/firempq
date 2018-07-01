package snsproto

import (
	"io"
	"net/http"

	"github.com/vburenin/firempq/qmgr"
	"github.com/vburenin/firempq/server/snsproto/create_topic"
	"github.com/vburenin/firempq/server/snsproto/list_topics"
	"github.com/vburenin/firempq/server/snsproto/sns_query"
	"github.com/vburenin/firempq/server/snsproto/sns_response"
	"github.com/vburenin/firempq/server/snsproto/snserr"
	"github.com/vburenin/firempq/server/snsproto/tmgr"
)

type SNSRequestHandler struct {
	ServiceManager *qmgr.QueueManager
}

func (self *SNSRequestHandler) dispatchSNSQuery(r *http.Request) sns_response.SNSResponse {
	q, err := sns_query.ParseSNSQuery(r)
	if err != nil {
		return snserr.MalformedRequestError()
	}
	tm := tmgr.TM()
	switch q.Action {
	case "CreateTopic":
		return create_topic.CreateTopic(tm, q)
	case "ListTopics":
		return list_topics.ListTopics(tm, q)
	}
	return nil
}

func (self *SNSRequestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	resp := self.dispatchSNSQuery(r)
	if resp == nil {
		return
	}

	w.WriteHeader(resp.HttpCode())
	io.WriteString(w, resp.XmlDocument())
	io.WriteString(w, "\n")
}
