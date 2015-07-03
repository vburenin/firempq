package pqueue

import "github.com/satori/go.uuid"

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

func NewPQMessage(payload string, priority int64) *PQMessage {
	id := uuid.NewV4().String()
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
