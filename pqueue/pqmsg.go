package pqueue

import (
	"firempq/defs"
	"firempq/qerrors"
	"firempq/util"
	"strconv"
)

const (
	MAX_MESSAGE_ID_LENGTH = 64
)

type Replica struct {
	ServerId  string
	ReplicaTs uint64
}

type PQMessage struct {
	Id           string
	Payload      string
	Priority     int64
	CreatedTs    int64
	PopCount     int64
	ReplicatedTo []Replica
}

func NewPQMessageWithId(id string, payload string, priority int64) *PQMessage {

	pqm := PQMessage{
		Id:           id,
		Payload:      payload,
		Priority:     priority,
		CreatedTs:    Uts(),
		PopCount:     0,
		ReplicatedTo: []Replica{},
	}
	return &pqm
}

func MessageFromMap(params map[string]string) (*PQMessage, error) {
	// Get and check message id.
	var msgId string
	var ok bool
	var strValue string
	var priority int64
	var err error
	var payload string

	msgId, ok = params[defs.PARAM_MSG_ID]
	if !ok {
		msgId = util.GenRandMsgId()
	} else if len(msgId) > MAX_MESSAGE_ID_LENGTH {
		return nil, qerrors.ERR_MSG_ID_TOO_LARGE
	}

	strValue, ok = params[defs.PARAM_MSG_PRIORITY]
	if !ok {
		return nil, qerrors.ERR_MSG_NO_PRIORITY
	}
	priority, err = strconv.ParseInt(strValue, 10, 0)
	if err != nil {
		return nil, qerrors.ERR_MSG_WRONG_PRIORITY
	}

	payload, ok = params[defs.PARAM_MSG_PAYLOAD]
	if !ok {
		payload = ""
	}

	return NewPQMessageWithId(msgId, payload, priority), nil
}

func NewPQMessage(payload string, priority int64) *PQMessage {
	id := util.GenRandMsgId()
	return NewPQMessageWithId(id, payload, priority)
}

func (pqm *PQMessage) AddReplica(serverId string, replicaTs uint64) {
	r := Replica{ServerId: serverId, ReplicaTs: replicaTs}
	pqm.ReplicatedTo = append(pqm.ReplicatedTo, r)
}

func (pqm *PQMessage) GetId() string {
	return pqm.Id
}

func (pqm *PQMessage) GetPayload() string {
	return pqm.Payload
}

func (pqm *PQMessage) GetStatus() map[string]interface{} {
	res := make(map[string]interface{})
	res["CreatedTs"] = pqm.CreatedTs
	res["Priority"] = pqm.Priority
	res["PopCount"] = pqm.PopCount
	return res
}

func (pqm *PQMessage) GetSize() int {
	return len(pqm.Payload)
}
