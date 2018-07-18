package tmgr

import (
	"sync"

	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/server/snsproto/arnutil"
	"github.com/vburenin/firempq/server/snsproto/dbdata"
	"go.uber.org/zap"
)

const TopicListPrefix = ":tlist:"
const TopicDataPrefix = ":tdata:"

type TopicManager struct {
	sync.Mutex
	Topics     map[string]*dbdata.Topic
	TargetArns map[string]*dbdata.Subscription
	TopicList  []string
}

const ReturnBatchSize = 5

func (tm *TopicManager) CreateTopic(topicName string) string {
	tm.Lock()
	defer tm.Unlock()
	topicArn := arnutil.MakeTopicArn(topicName)

	topic := tm.Topics[topicArn]
	if topic != nil {
		return topicArn
	}
	topic = &dbdata.Topic{
		Arn:  topicArn,
		Name: topicName,
	}
	tm.TopicList = append(tm.TopicList, topicArn)
	tm.Topics[topicArn] = topic
	return topicArn
}

func (tm *TopicManager) ListTopics(offset int) ([]string, int) {
	tm.Lock()
	defer tm.Unlock()
	var res []string

	maxLen := offset + ReturnBatchSize
	ret_offset := maxLen

	if maxLen >= len(tm.TopicList) {
		ret_offset = 0
		maxLen = len(tm.TopicList)
	}

	for i := offset; i < maxLen; i++ {
		topicName := tm.TopicList[i]
		if t := tm.Topics[topicName]; t == nil {
			log.Error("no data for topic name", zap.String("topic", topicName))
		} else {
			res = append(res, t.Arn)
		}
	}
	return res, ret_offset
}

func (tm *TopicManager) DeleteTopic(topicArn string) bool {
	tm.Lock()
	defer tm.Unlock()

	t := tm.Topics[topicArn]
	if t == nil {
		return false
	}
	for k := range t.OtherSubscriptions {
		delete(tm.TargetArns, k)
	}
	for k := range t.SqsSubscriptions {
		delete(tm.TargetArns, k)
	}

	delete(tm.Topics, topicArn)

	// Uh oh, how inefficient this is...
	res := make([]string, 0, len(tm.Topics))
	for k := range tm.Topics {
		res = append(res, k)
	}

	tm.TopicList = res
	return true
}

func (tm *TopicManager) Subscribe(topicArn, protocol, endpoint string) string {
	tm.Lock()
	defer tm.Unlock()

	targetArn := arnutil.MakeTargetArn(topicArn, protocol, endpoint)

	s := tm.TargetArns[targetArn]
	if s != nil {
		return targetArn
	}

	t := tm.Topics[topicArn]
	if t == nil {
		return ""
	}

	ss := &dbdata.Subscription{
		Endpoint: endpoint,
		Protocol: protocol,
		Arn:      targetArn,
	}

	tm.TargetArns[targetArn] = ss
	if protocol == "sqs" {
		t.SqsSubscriptions[targetArn] = ss
	} else {
		t.OtherSubscriptions[targetArn] = ss
	}

	return targetArn
}

type DataToPublish struct {
	TopicArn          string
	TargetArn         string
	Message           string
	Subject           string
	MessageStructure  string
	MessageAttributes map[string]map[string]string
}

func (tm *TopicManager) Publish(data *DataToPublish) {

}

var tm *TopicManager
var tmgr sync.Once

func TM() *TopicManager {
	tmgr.Do(func() {
		tm = &TopicManager{
			Topics:     make(map[string]*dbdata.Topic),
			TargetArns: make(map[string]*dbdata.Subscription),
		}
	})
	return tm
}
