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

func (pqm *PQMessage) AddReplica(serverId string, replicaTs uint64) {
	r := Replica{ServerId: serverId, ReplicaTs: replicaTs}
	pqm.ReplicatedTo = append(pqm.ReplicatedTo, r)
}

func NewPQMessageWithId(id string, payload string, priority int64) *PQMessage {

	pqm := PQMessage{
        Id: id,
		Payload:      payload,
		Priority:     priority,
		CreatedTs:    Uts(),
		PopCount:   0,
		ReplicatedTo: []Replica{},
	}
	return &pqm
}

func NewPQMessage(payload string, priority int64) *PQMessage {
	id := uuid.NewV4().String()
	return NewPQMessageWithId(id, payload, priority)
}
