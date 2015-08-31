package pqueue

import (
	"firempq/common"
	"firempq/db"
	"firempq/defs"
	"firempq/structs"
	"firempq/svcerr"
	"firempq/util"
	"fmt"
	"github.com/op/go-logging"
	"sort"
	"sync"
	"time"
)

var log = logging.MustGetLogger("firempq")

// Size of one batch of messages for popMessageBatch function
const MESSAGES_BATCH_SIZE_LIMIT = 20
const MAX_POP_WAIT_TIMEOUT = 30000

const (
	ACTION_UNLOCK_BY_ID        = "UNLOCK"
	ACTION_DELETE_LOCKED_BY_ID = "DELLOCKED"
	ACTION_DELETE_BY_ID        = "DEL"
	ACTION_SET_LOCK_TIMEOUT    = "SETLOCKTIMEOUT"
	ACTION_PUSH                = "PUSH"
	ACTION_POP                 = "POP"
	ACTION_POP_WAIT            = "POPWAIT"
	ACTION_SET_QPARAM          = "SETQP"
	ACTION_SET_MPARAM          = "SETMP"
)

type PQueue struct {
	queueName string
	// Messages which are waiting to be picked up
	availableMsgs *structs.PriorityFirstQueue

	// All messages with the ticking counters except those which are inFlight.
	expireHeap *structs.IndexHeap
	// All locked messages
	inFlightHeap *structs.IndexHeap
	// Just a message map message id to the full message data.
	allMessagesMap map[string]*PQMessage

	lock sync.Mutex
	// Used to stop internal goroutine.
	workDone bool
	// Action handlers which are out of standard queue interface.
	actionHandlers map[string](common.CallFuncType)
	// Instance of the database.
	database *db.DataStorage
	// Queue settings.
	settings *PQueueSettings

	newMsgNotification chan bool
}

func initPQueue(database *db.DataStorage, queueName string, settings *PQueueSettings) *PQueue {
	pq := PQueue{
		allMessagesMap:     make(map[string]*PQMessage),
		availableMsgs:      structs.NewActiveQueues(settings.MaxPriority),
		database:           database,
		expireHeap:         structs.NewIndexHeap(),
		inFlightHeap:       structs.NewIndexHeap(),
		queueName:          queueName,
		settings:           settings,
		workDone:           false,
		newMsgNotification: make(chan bool),
	}

	pq.loadAllMessages()
	go pq.periodicCleanUp()

	return &pq
}

func NewPQueue(database *db.DataStorage, queueName string, priorities int64, size int64) *PQueue {
	settings := NewPQueueSettings(priorities, size)
	queue := initPQueue(database, queueName, settings)
	queue.database.SaveServiceConfig(queueName, settings)
	return queue
}

func LoadPQueue(database *db.DataStorage, queueName string) (common.ISvc, error) {
	settings := new(PQueueSettings)
	err := database.GetServiceConfig(settings, queueName)
	if err != nil {
		return nil, err
	}
	pq := initPQueue(database, queueName, settings)
	return pq, nil
}

func (pq *PQueue) GetStatus() map[string]interface{} {
	res := pq.settings.ToMap()
	res["TotalMessages"] = len(pq.allMessagesMap)
	res["InFlightSize"] = pq.inFlightHeap.Len()
	return res
}

func (pq *PQueue) GetType() defs.ServiceType {
	return defs.HT_PRIORITY_QUEUE
}

func (pq *PQueue) GetTypeName() string {
	return common.STYPE_PRIORITY_QUEUE
}

// Call dispatches processing of the command to the appropriate command parser.
func (pq *PQueue) Call(cmd string, params []string) *common.ReturnData {
	switch cmd {
	case ACTION_POP_WAIT:
		return pq.PopWait(params)
	case ACTION_DELETE_LOCKED_BY_ID:
		return pq.DeleteLockedById(params)
	case ACTION_DELETE_BY_ID:
		return pq.DeleteById(params)
	case ACTION_POP:
		return pq.Pop(params)
	case ACTION_PUSH:
		return pq.Push(params)
	case ACTION_SET_LOCK_TIMEOUT:
		return pq.SetLockTimeout(params)
	case ACTION_UNLOCK_BY_ID:
		return pq.UnlockMessageById(params)
	}
	return common.NewRetDataError(svcerr.InvalidRequest("Unknown action: " + cmd))
}

// Delete all messages in the queue. It includes all type of messages
func (pq *PQueue) Clear() {
	total := 0
	for {
		ids := []string{}
		pq.lock.Lock()
		if len(pq.allMessagesMap) == 0 {
			pq.lock.Unlock()
			break
		}
		for k, _ := range pq.allMessagesMap {
			ids = append(ids, k)
			if len(ids) > 100 {
				break
			}
		}
		total += len(ids)
		for _, id := range ids {
			pq.deleteMessage(id)
		}
		pq.lock.Unlock()
	}
	log.Debug("Removed %d messages.", total)
}

func (pq *PQueue) Close() {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	if pq.workDone {
		return
	}
	pq.workDone = true
}

func (pq *PQueue) IsClosed() bool {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	return pq.workDone
}

func makeUnknownParamResponse(paramName string) *common.ReturnData {
	err := svcerr.InvalidRequest(fmt.Sprintf("Unknown parameter: %s", paramName))
	return common.NewRetDataError(err)
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (pq *PQueue) Push(params []string) *common.ReturnData {
	var err error
	var msgId string
	var priority int64 = pq.settings.MaxPriority - 1
	var payload string = ""

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = util.ParseStringParam(params, 1, 128)
		case PRM_PRIORITY:
			params, priority, err = util.ParseInt64Params(params, 0, pq.settings.MaxPriority-1)
		case PRM_PAYLOAD:
			params, payload, err = util.ParseStringParam(params, 1, 512*1024)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return common.NewRetDataError(err)
		}
	}
	if len(msgId) == 0 {
		msgId = util.GenRandMsgId()
	}

	msg := NewPQMessageWithId(msgId, priority)

	pq.settings.LastPushTs = util.Uts()

	pq.lock.Lock()
	defer pq.lock.Unlock()
	return pq.storeMessage(msg, payload)
}

func (pq *PQueue) storeMessage(msg *PQMessage, payload string) *common.ReturnData {
	if _, ok := pq.allMessagesMap[msg.Id]; ok {
		return common.NewRetDataError(svcerr.ERR_ITEM_ALREADY_EXISTS)
	}
	queueLen := pq.availableMsgs.Len()
	pq.allMessagesMap[msg.Id] = msg
	pq.trackExpiration(msg)
	pq.availableMsgs.Push(msg.Id, msg.Priority)
	if 0 == queueLen {
		select {
		case pq.newMsgNotification <- true:
		default: // allows non blocking channel usage if there are no users awaiting wor the message
		}
	}

	pq.database.StoreItemWithPayload(pq.queueName, msg, payload)
	return common.RETDATA_201OK
}

// Pop first available messages.
// Will return nil if there are no messages available.
func (pq *PQueue) Pop(params []string) *common.ReturnData {
	var err error
	var limit int64 = 1
	lockTimeout := pq.settings.PopLockTimeout

	for len(params) > 0 {
		switch params[0] {
		case PRM_LOCK_TIMEOUT:
			params, lockTimeout, err = util.ParseInt64Params(params, 0, 24*1000*3600)
		case PRM_LIMIT:
			params, limit, err = util.ParseInt64Params(params, 1, MESSAGES_BATCH_SIZE_LIMIT)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return common.NewRetDataError(err)
		}
	}
	return pq.popMessages(lockTimeout, limit)
}

// PopWait pops up to specified number of messages, if there are no available messages.
// It will wait until either new message is pushed or wait time exceeded.
func (pq *PQueue) PopWait(params []string) *common.ReturnData {

	var err error
	var limit int64 = 1
	var popWaitTimeout int64 = 1000
	var lockTimeout int64 = pq.settings.PopLockTimeout

	for len(params) > 0 {
		switch params[0] {
		case PRM_LOCK_TIMEOUT:
			params, lockTimeout, err = util.ParseInt64Params(params, 0, 24*1000*3600)
		case PRM_LIMIT:
			params, limit, err = util.ParseInt64Params(params, 1, MESSAGES_BATCH_SIZE_LIMIT)
		case PRM_POP_WAIT_TIMEOUT:
			params, popWaitTimeout, err = util.ParseInt64Params(params, 1, MAX_POP_WAIT_TIMEOUT)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return common.NewRetDataError(err)
		}
	}

	return pq.popWaitItems(lockTimeout, popWaitTimeout, limit)
}

func (pq *PQueue) popMetaMessage(lockTimeout int64) *PQMessage {
	if pq.availableMsgs.Empty() {
		return nil
	}

	msgId := pq.availableMsgs.Pop()
	msg, ok := pq.allMessagesMap[msgId]

	if !ok {
		return nil
	}
	msg.PopCount += 1
	pq.lockMessage(msg, lockTimeout)
	pq.database.UpdateItem(pq.queueName, msg)

	return msg
}

func (pq *PQueue) popMessages(lockTimeout int64, limit int64) *common.ReturnData {
	var msgsMeta []*PQMessage
	pq.lock.Lock()
	for int64(len(msgsMeta)) < limit {
		mm := pq.popMetaMessage(lockTimeout)
		if mm == nil {
			break
		} else {
			msgsMeta = append(msgsMeta, mm)
		}
	}
	pq.lock.Unlock()

	var msgs []common.IItem
	for _, mm := range msgsMeta {
		msgs = append(msgs, NewMsgItem(mm, pq.getPayload(mm.Id)))
	}

	return common.NewRetData("Ok", defs.CODE_200_OK, msgs)
}

// Will pop 'limit' messages within 'timeout'(milliseconds) time interval.
func (pq *PQueue) popWaitItems(lockTimeout, popWaitTimeout, limit int64) *common.ReturnData {
	// Try to pop items first time and return them if number of popped items is greater than 0.
	msgItems := pq.popMessages(lockTimeout, limit)
	if len(msgItems.Items) > 0 {
		return msgItems
	}

	// If items not founds lets wait for any incoming.
	timeoutChan := make(chan bool)
	defer close(timeoutChan)

	go func() {
		time.Sleep(time.Duration(popWaitTimeout) * time.Millisecond)
		timeoutChan <- true
	}()

	for {
		select {
		case t := <-pq.newMsgNotification:
			msgItems := pq.popMessages(lockTimeout, limit)
			if len(msgItems.Items) > 0 && t {
				return msgItems
			}
		case t := <-timeoutChan:
			if t {
				return pq.popMessages(lockTimeout, limit)
			}
		}
	}
	return msgItems
}

// Returns message payload.
// This method doesn't lock anything. Actually lock happens withing loadPayload structure.
func (pq *PQueue) getPayload(msgId string) string {
	return pq.database.GetPayload(pq.queueName, msgId)
}

func (pq *PQueue) lockMessage(msg *PQMessage, lockTimeout int64) {
	nowTs := util.Uts()
	pq.settings.LastPopTs = nowTs
	// Increase number of pop attempts.

	msg.UnlockTs = nowTs + lockTimeout
	pq.expireHeap.PopById(msg.Id)
	pq.inFlightHeap.PushItem(msg.Id, msg.UnlockTs)
}

// Remove message id from In Flight message heap.
func (pq *PQueue) unflightMessage(msgId string) (*PQMessage, error) {
	msg, ok := pq.allMessagesMap[msgId]
	if !ok {
		return nil, svcerr.ERR_MSG_NOT_EXIST
	}

	hi := pq.inFlightHeap.PopById(msgId)
	if hi == structs.EMPTY_HEAP_ITEM {
		return nil, svcerr.ERR_MSG_NOT_LOCKED
	}

	return msg, nil
}

func (pq *PQueue) DeleteById(params []string) *common.ReturnData {
	var err error
	var msgId string

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = util.ParseStringParam(params, 1, 128)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return common.NewRetDataError(err)
		}
	}

	if len(msgId) == 0 {
		return common.NewRetDataError(svcerr.ERR_MSG_ID_NOT_DEFINED)
	}
	pq.lock.Lock()
	defer pq.lock.Unlock()

	if pq.inFlightHeap.ContainsId(msgId) {
		return common.NewRetDataError(svcerr.ERR_MSG_IS_LOCKED)
	}

	if !pq.deleteMessage(msgId) {
		return common.NewRetDataError(svcerr.ERR_MSG_NOT_EXIST)
	}

	return common.RETDATA_201OK
}

// Set a user defined message lock timeout. Only locked message timeout can be set.
func (pq *PQueue) SetLockTimeout(params []string) *common.ReturnData {
	var err error
	var msgId string
	var lockTimeout int64 = -1

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = util.ParseStringParam(params, 1, 128)
		case PRM_LOCK_TIMEOUT:
			params, lockTimeout, err = util.ParseInt64Params(params, 0, 24*1000*3600)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return common.NewRetDataError(err)
		}
	}

	if len(msgId) == 0 {
		return common.NewRetDataError(svcerr.ERR_MSG_ID_NOT_DEFINED)
	}

	if lockTimeout < 0 {
		return common.NewRetDataError(svcerr.ERR_MSG_TIMEOUT_NOT_DEFINED)
	}

	pq.lock.Lock()
	defer pq.lock.Unlock()

	msg, err := pq.unflightMessage(msgId)
	if err != nil {
		return common.NewRetDataError(err)
	}

	msg.UnlockTs = util.Uts() + int64(lockTimeout)

	pq.inFlightHeap.PushItem(msgId, msg.UnlockTs)
	pq.database.UpdateItem(pq.queueName, msg)

	return common.RETDATA_201OK
}

// Delete locked message by id.
func (pq *PQueue) DeleteLockedById(params []string) *common.ReturnData {
	var err error
	var msgId string

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = util.ParseStringParam(params, 1, 128)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return common.NewRetDataError(err)
		}
	}

	if len(msgId) == 0 {
		return common.NewRetDataError(svcerr.ERR_MSG_ID_NOT_DEFINED)
	}

	pq.lock.Lock()
	defer pq.lock.Unlock()

	_, err = pq.unflightMessage(msgId)
	if err != nil {
		return common.NewRetDataError(err)
	}
	pq.deleteMessage(msgId)
	return common.RETDATA_201OK
}

func (pq *PQueue) UnlockMessageById(params []string) *common.ReturnData {
	var err error
	var msgId string

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = util.ParseStringParam(params, 1, 128)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return common.NewRetDataError(err)
		}
	}

	if len(msgId) == 0 {
		return common.NewRetDataError(svcerr.ERR_MSG_ID_NOT_DEFINED)
	}

	pq.lock.Lock()
	defer pq.lock.Unlock()

	// Make sure message exists.
	msg, e := pq.unflightMessage(msgId)
	if e != nil {
		return common.NewRetDataError(err)
	}
	// Message exists, push it into the front of the queue.
	err = pq.returnToFront(msg)
	if err != nil {
		return common.NewRetDataError(err)
	}
	return common.RETDATA_201OK
}

func (pq *PQueue) deleteMessage(msgId string) bool {
	if msg, ok := pq.allMessagesMap[msgId]; ok {
		delete(pq.allMessagesMap, msgId)
		pq.availableMsgs.RemoveItem(msgId, msg.Priority)
		pq.database.DeleteItem(pq.queueName, msgId)
		pq.expireHeap.PopById(msgId)
		return true
	}
	return false
}

// Adds message into expiration heap. Not thread safe!
func (pq *PQueue) trackExpiration(msg *PQMessage) {
	ok := pq.expireHeap.PushItem(msg.Id, msg.CreatedTs+int64(pq.settings.MsgTTL))
	if !ok {
		log.Error("Error! Item already exists in the expire heap: %s", msg.Id)
	}
}

// Attempts to return a message into the front of the queue.
// If a number of POP attempts has exceeded, message will be deleted.
func (pq *PQueue) returnToFront(msg *PQMessage) error {
	lim := pq.settings.PopCountLimit
	if lim > 0 {
		if lim <= msg.PopCount {
			pq.deleteMessage(msg.Id)
			return svcerr.ERR_MSG_POP_ATTEMPTS_EXCEEDED
		}
	}
	msg.UnlockTs = 0
	pq.availableMsgs.PushFront(msg.Id)
	pq.trackExpiration(msg)
	pq.database.UpdateItem(pq.queueName, msg)
	return nil
}

// Unlocks all items which exceeded their lock time.
func (pq *PQueue) releaseInFlight() int {
	cur_ts := util.Uts()
	ifHeap := pq.inFlightHeap

	i := 0
	for !(ifHeap.Empty()) && ifHeap.MinElement() < cur_ts {
		i++
		hi := ifHeap.PopItem()
		pqmsg := pq.allMessagesMap[hi.Id]
		pq.returnToFront(pqmsg)
		if i >= MAX_CLEANS_PER_ATTEMPT {
			break
		}
	}
	return i
}

// Remove all items which are completely expired.
func (pq *PQueue) cleanExpiredItems() int {
	cur_ts := util.Uts()
	eh := pq.expireHeap

	i := 0
	for !(eh.Empty()) && eh.MinElement() < cur_ts {
		i++
		hi := eh.PopItem()
		// There are two places to remove expired item:
		// 1. Map of all items - all items map.
		// 2. Available message.
		msg := pq.allMessagesMap[hi.Id]
		pq.availableMsgs.RemoveItem(msg.Id, msg.Priority)
		pq.deleteMessage(msg.Id)
		if i >= MAX_CLEANS_PER_ATTEMPT {
			break
		}
	}
	return i
}

// 1 milliseconds.
const SLEEP_INTERVAL_IF_ITEMS = 1000000

// 1000 items should be enough to not create long locks. In the most good cases clean ups are rare.
const MAX_CLEANS_PER_ATTEMPT = 1000

// How frequently loop should run.
const DEFAULT_UNLOCK_INTERVAL = 100 * 1000000 // 0.1 second

// Remove expired items. Should be running as a thread.
func (pq *PQueue) periodicCleanUp() {
	for !(pq.workDone) {
		var sleepTime time.Duration = DEFAULT_UNLOCK_INTERVAL

		pq.lock.Lock()
		if !(pq.workDone) {
			cleaned := pq.releaseInFlight()
			if cleaned > 0 {
				log.Debug("%d messages returned to the front of the queue.", cleaned)
				sleepTime = SLEEP_INTERVAL_IF_ITEMS
			}
		}
		pq.lock.Unlock()

		pq.lock.Lock()
		if !(pq.workDone) {
			cleaned := pq.cleanExpiredItems()
			if cleaned > 0 {
				log.Debug("%d items expired.", cleaned)
				sleepTime = SLEEP_INTERVAL_IF_ITEMS
			}
		}
		pq.lock.Unlock()
		if !pq.workDone {
			time.Sleep(sleepTime)
		}
	}
}

// Database related data management.
type MessageSlice []*PQMessage

func (p MessageSlice) Len() int           { return len(p) }
func (p MessageSlice) Less(i, j int) bool { return p[i].CreatedTs < p[j].CreatedTs }
func (p MessageSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (pq *PQueue) loadAllMessages() {
	nowTs := util.Uts()
	log.Info("Initializing queue: %s", pq.queueName)
	iter := pq.database.IterServiceItems(pq.queueName)
	defer iter.Close()

	msgs := MessageSlice{}
	delIds := []string{}

	s := pq.settings
	for iter.Valid() {
		pqmsg := PQMessageFromBinary(string(iter.Key), iter.Value)

		// Store list if message IDs that should be removed.
		if pqmsg.CreatedTs+s.MsgTTL < nowTs ||
			(pqmsg.PopCount >= s.PopCountLimit &&
				s.PopCountLimit > 0) {
			delIds = append(delIds, pqmsg.Id)
		} else {
			msgs = append(msgs, pqmsg)
		}
		iter.Next()
	}
	log.Debug("Loaded %d messages for %s queue", len(msgs), pq.queueName)
	if len(delIds) > 0 {
		log.Debug("%d messages will be removed because of expiration", len(delIds))
		for _, msgId := range delIds {
			pq.database.DeleteItem(pq.queueName, msgId)
		}

	}
	// Sorting data guarantees that messages will be available almost in the same order as they arrived.
	sort.Sort(msgs)

	for _, msg := range msgs {
		pq.allMessagesMap[msg.Id] = msg
		if msg.UnlockTs > nowTs {
			pq.inFlightHeap.PushItem(msg.Id, msg.UnlockTs)
		} else {
			pq.expireHeap.PushItem(msg.Id, msg.CreatedTs+s.MsgTTL)
			pq.availableMsgs.Push(msg.Id, msg.Priority)
		}
	}

	log.Debug("Messages available: %d", pq.expireHeap.Len())
	log.Debug("Messages in flight: %d", pq.inFlightHeap.Len())
}

var _ common.ISvc = &PQueue{}
