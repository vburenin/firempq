package client

import (
	"github.com/vburenin/firempq/export/proto"
)

type Queue struct {
	t         *TokenUtil
	queueName string
	asyncPop  map[string]func([]QueueMessage, error)
}

type PushBatchItem struct {
	Error error
	MsgID string
}

func SetContext(queueName string, t *TokenUtil) (*Queue, error) {
	err := t.SendCompleteTokenWithString(proto.CmdCtx, queueName)
	if err != nil {
		return nil, err
	}

	if err = ExpectOk(t); err != nil {
		return nil, err
	}

	pq := &Queue{
		t:         t,
		queueName: queueName,
	}
	return pq, nil
}

func CreateQueue(queueName string, t *TokenUtil, opts *QueueParams) (*Queue, error) {
	t.SendTokenWithStringParam(proto.CmdCreateQueue, queueName)
	opts.sendOptions(t)

	if err := t.Complete(); err != nil {
		return nil, err
	}

	if err := ExpectOk(t); err != nil {
		return nil, err
	}

	return SetContext(queueName, t)
}

func (pq *Queue) Name() string {
	return pq.queueName
}

func (pq *Queue) NewMessage(payload string) *Message {
	return NewMessage(payload)
}

func (pq *Queue) PushBatch(msgs ...*Message) ([]PushBatchItem, error) {
	last := len(msgs) - 1
	if last == -1 {
		return nil, nil
	}
	pushCmd := proto.CmdPushBatch
	for i, msg := range msgs {
		pq.t.SendToken(pushCmd)
		msg.sendMessageData(pq.t)
		if i != last {
			pq.t.SendSpace()
		}
		pushCmd = proto.CmdBatchNext
	}
	err := pq.t.Complete()
	if err != nil {
		return nil, err
	}
	return pq.handleBatchResponse()
}

func (pq *Queue) Push(msg *Message) (string, error) {
	pq.t.SendToken(proto.CmdPush)
	msg.sendMessageData(pq.t)
	err := pq.t.Complete()
	if err != nil {
		return "", err
	}
	tokens, err := pq.t.ReadTokens()
	if err != nil {
		return "", err
	}
	return ParseMessageId(tokens)
}

// Pop pops available from the queue completely removing them.
func (pq *Queue) Pop(opts *popOptions) ([]*QueueMessage, error) {
	pq.t.SendToken(proto.CmdPop)
	opts.sendOptions(pq.t)
	err := pq.t.Complete()
	if err != nil {
		return nil, err
	}
	return pq.handleMessages()
}

// PopLock pops available from the queue locking them.
func (pq *Queue) PopLock(opts *popLockOptions) ([]*QueueMessage, error) {
	pq.t.SendToken(proto.CmdPopLock)
	opts.sendOptions(pq.t)
	err := pq.t.Complete()
	if err != nil {
		return nil, err
	}
	return pq.handleMessages()
}

func (pq *Queue) DeleteById(id string) error {
	err := pq.t.SendCompleteTokenWithString(proto.CmdDeleteByID, id)
	if err != nil {
		return err
	}
	return ExpectOk(pq.t)
}

func (pq *Queue) DeleteLockedById(id string) error {
	err := pq.t.SendCompleteTokenWithString(proto.CmdDeleteLockedByID, id)
	if err != nil {
		return err
	}
	return ExpectOk(pq.t)
}

func (pq *Queue) DeleteByReceipt(rcpt string) error {
	err := pq.t.SendCompleteTokenWithString(proto.CmdDeleteByRcpt, rcpt)
	if err != nil {
		return err
	}
	return ExpectOk(pq.t)
}

func (pq *Queue) UnlockById(id string) error {
	err := pq.t.SendCompleteTokenWithString(proto.CmdUnlockByID, id)
	if err != nil {
		return err
	}
	return ExpectOk(pq.t)
}

func (pq *Queue) UnlockByReceipt(rcpt string) error {
	err := pq.t.SendCompleteTokenWithString(proto.CmdUnlockByRcpt, rcpt)
	if err != nil {
		return err
	}
	return ExpectOk(pq.t)
}

func (pq *Queue) SetParams(params *QueueParams) error {
	pq.t.SendToken(proto.CmdSetConfig)
	params.sendOptions(pq.t)
	if err := pq.t.Complete(); err != nil {
		return err
	}
	return ExpectOk(pq.t)
}

func (pq *Queue) handleMessages() ([]*QueueMessage, error) {
	tokens, err := pq.t.ReadTokens()

	if err != nil {
		return nil, err
	}

	if tokens[0] == "+MSGS" {
		return parsePoppedMessages(tokens[1:])
	}

	if err := ParseError(tokens); err != nil {
		return nil, err
	}

	return nil, UnexpectedResponse(tokens)
}

func (pq *Queue) handleBatchResponse() ([]PushBatchItem, error) {
	tokens, err := pq.t.ReadTokens()
	if err != nil {
		return nil, err
	}
	if len(tokens) < 2 {
		return nil, UnexpectedResponse(tokens)
	}
	if tokens[0] == "+BATCH" {
		size, err := ParseArraySize(tokens[1])
		if err != nil {
			return nil, err
		}
		if size < 0 {
			return nil, UnexpectedResponse(tokens)
		}
		return pq.parseBatchResponse(int(size))

	}
	if err := ParseError(tokens); err != nil {
		return nil, err
	}

	return nil, UnexpectedResponse(tokens)
}

func (pq *Queue) parseBatchResponse(size int) ([]PushBatchItem, error) {
	respItems := make([]PushBatchItem, 0, size)
	var id string
	for len(respItems) < size {
		tokens, err := pq.t.ReadTokens()
		if err != nil {
			return nil, err
		}

		err = ParseError(tokens)
		if err != nil {
			respItems = append(respItems, PushBatchItem{Error: err})
			continue
		}

		id, err = ParseMessageId(tokens)
		if id != "" {
			respItems = append(respItems, PushBatchItem{MsgID: id})
			tokens = tokens[2:]
			continue
		}
		return nil, err
	}
	return respItems, nil
}
