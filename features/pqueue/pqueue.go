package pqueue

import (
	"firempq/common"
	"firempq/db"
	"firempq/defs"
	"firempq/qerrors"
	"firempq/structs"
	"firempq/util"
	"github.com/op/go-logging"
	"sort"
	"strconv"
	"sync"
	"time"
)

var log = logging.MustGetLogger("firempq")

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

	pq.actionHandlers = map[string](common.CallFuncType){
		ACTION_DELETE_LOCKED_BY_ID: pq.DeleteLockedById,
		ACTION_SET_LOCK_TIMEOUT:    pq.SetLockTimeout,
		ACTION_UNLOCK_BY_ID:        pq.UnlockMessageById,
		ACTION_PUSH:                pq.Push,
		ACTION_POP:                 pq.Pop,
		ACTION_DELETE_BY_ID:        pq.DeleteById,
		ACTION_POP_WAIT:            pq.PopWait,
	}

	pq.loadAllMessages()
	go pq.periodicCleanUp()

	return &pq
}

func NewPQueue(database *db.DataStorage, queueName string, priorities int64, size int64) *PQueue {
	settings := NewPQueueSettings(priorities, size)
	queue := initPQueue(database, queueName, settings)
	queue.database.SaveQueueSettings(queueName, settings)
	return queue
}

func LoadPQueue(database *db.DataStorage, queueName string) (common.IItemHandler, error) {
	settings := new(PQueueSettings)
	err := database.GetQueueSettings(settings, queueName)
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

func (pq *PQueue) GetType() defs.ItemHandlerType {
	return defs.HT_PRIORITY_QUEUE
}

// Queue custom specific handler for the queue type specific features.
func (pq *PQueue) Call(action string, params map[string]string) *common.ReturnData {
	handler, ok := pq.actionHandlers[action]
	if !ok {
		return common.NewRetDataError(qerrors.InvalidRequest("Unknown action: " + action))
	}
	return handler(params)
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
	if pq.workDone {
		return
	}
	pq.workDone = true
	pq.lock.Unlock()
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (pq *PQueue) Push(msgData map[string]string) *common.ReturnData {
	msg, err := MessageFromMap(msgData)
	if err != nil {
		return common.NewRetDataError(err)
	}
	payload, ok := msgData[defs.PRM_PAYLOAD]
	if !ok {
		payload = ""
	}
	pq.settings.LastPushTs = util.Uts()

	pq.lock.Lock()
	defer pq.lock.Unlock()
	return pq.storeMessage(msg, payload)
}

func (pq *PQueue) storeMessage(msg *PQMessage, payload string) *common.ReturnData {
	if _, ok := pq.allMessagesMap[msg.Id]; ok {
		return common.NewRetDataError(qerrors.ERR_ITEM_ALREADY_EXISTS)
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

func (pq *PQueue) popMetaMessage() *PQMessage {
	if pq.availableMsgs.Empty() {
		return nil
	}

	msgId := pq.availableMsgs.Pop()
	msg, ok := pq.allMessagesMap[msgId]

	if !ok {
		return nil
	}
	msg.PopCount += 1
	pq.lockMessage(msg)
	pq.database.UpdateItem(pq.queueName, msg)

	return msg
}

// Pop first available message.
// Will return nil if there are no messages available.
func (pq *PQueue) Pop(params map[string]string) *common.ReturnData {

	pq.lock.Lock()
	msg := pq.popMetaMessage()
	pq.lock.Unlock()

	if msg == nil {
		return common.NewRetDataError(qerrors.ERR_QUEUE_EMPTY)
	}

	retMsg := NewMsgItem(msg, pq.getPayload(msg.Id))

	return common.NewRetData("Ok", defs.CODE_200_OK, []common.IItem{retMsg})
}

// Size of one batch of messages for popMessageBatch function
const MESSAGES_BATCH_SIZE_LIMIT = 20

func (pq *PQueue) PopWait(params map[string]string) *common.ReturnData {
	var timeout int = 10
	var limit int = 0
	var err error
	var ok bool
	var timeoutStr string
	var limitStr string

	timeoutStr, ok = params[defs.PRM_TIMEOUT]
	if ok {
		timeout, err = strconv.Atoi(timeoutStr)
		if err != nil {
			return common.NewRetDataError(err)
		}
		if timeout < 0 || timeout > 20000 {
			return common.NewRetDataError(qerrors.ERR_MSG_TIMEOUT_IS_WRONG)
		}
	}

	limitStr, ok = params[defs.PRM_POP_LIMIT]
	if ok {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			return common.NewRetDataError(err)
		}
		if limit < 1 || limit > MESSAGES_BATCH_SIZE_LIMIT {
			return common.NewRetDataError(qerrors.ERR_MSG_LIMIT_IS_WRONG)
		}
	}
	msgItems := pq.popWaitItems(timeout, limit)
	if len(msgItems) == 0 {
		return common.NewRetDataError(qerrors.ERR_QUEUE_EMPTY)
	}

	return common.NewRetData("OK", defs.CODE_200_OK, msgItems)
}

// Will pop 'limit' messages within 'timeout'(milliseconds) time interval.
func (pq *PQueue) popWaitItems(timeout, limit int) []common.IItem {
	msgItems := make([]common.IItem, 0, limit)

	popLimitItems := func() {
		for len(msgItems) < limit {
			pq.lock.Lock()
			msg := pq.popMetaMessage()
			pq.lock.Unlock()
			if msg == nil {
				break
			}
			msgItem := NewMsgItem(msg, pq.getPayload(msg.Id))
			msgItems = append(msgItems, msgItem)
		}
	}
	// Try to pop items first time and return them if number of popped items is greater than 0.
	popLimitItems()
	if len(msgItems) > 0 {
		common.NewRetData("OK", defs.CODE_200_OK, msgItems)
		return msgItems
	}
	// If items not founds lets wait for any incoming.
	timeoutChan := make(chan bool)
	defer close(timeoutChan)

	go func() {
		time.Sleep(time.Duration(timeout) * time.Millisecond)
		timeoutChan <- true
	}()

	needExit := false
	for !needExit && len(msgItems) == 0 {
		select {
		case t := <-pq.newMsgNotification:
			popLimitItems()
			needExit = len(msgItems) == 0 && t
		case t := <-timeoutChan:
			needExit = true || t
			popLimitItems()
		}
	}

	return msgItems
}

// Returns message payload.
// This method doesn't lock anything. Actually lock happens withing loadPayload structure.
func (pq *PQueue) getPayload(msgId string) string {
	return pq.database.GetPayload(pq.queueName, msgId)
}

func (pq *PQueue) lockMessage(msg *PQMessage) {
	nowTs := util.Uts()
	pq.settings.LastPopTs = nowTs
	// Increase number of pop attempts.

	msg.UnlockTs = nowTs + pq.settings.PopLockTimeout
	pq.expireHeap.PopById(msg.Id)
	pq.inFlightHeap.PushItem(msg.Id, msg.UnlockTs)
}

// Remove message id from In Flight message heap.
func (pq *PQueue) unflightMessage(msgId string) (*PQMessage, error) {
	msg, ok := pq.allMessagesMap[msgId]
	if !ok {
		return nil, qerrors.ERR_MSG_NOT_EXIST
	}

	hi := pq.inFlightHeap.PopById(msgId)
	if hi == structs.EMPTY_HEAP_ITEM {
		return nil, qerrors.ERR_MSG_NOT_LOCKED
	}

	return msg, nil
}

func (pq *PQueue) DeleteById(params map[string]string) *common.ReturnData {
	msgId, ok := params[defs.PRM_ID]
	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_ID_NOT_DEFINED)
	}
	pq.lock.Lock()
	defer pq.lock.Unlock()

	if pq.inFlightHeap.ContainsId(msgId) {
		return common.NewRetDataError(qerrors.ERR_MSG_IS_LOCKED)
	}

	if !pq.deleteMessage(msgId) {
		return common.NewRetDataError(qerrors.ERR_MSG_NOT_EXIST)
	}

	return common.RETDATA_201OK
}

// Set a user defined message lock timeout. Only locked message timeout can be set.
func (pq *PQueue) SetLockTimeout(params map[string]string) *common.ReturnData {
	msgId, ok := params[defs.PRM_ID]
	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_ID_NOT_DEFINED)
	}

	timeoutStr, ok := params[defs.PRM_TIMEOUT]

	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_TIMEOUT_NOT_DEFINED)
	}

	timeout, terr := strconv.Atoi(timeoutStr)
	if terr != nil || timeout < 0 || timeout > defs.TIMEOUT_MAX_LOCK {
		return common.NewRetDataError(qerrors.ERR_MSG_TIMEOUT_IS_WRONG)
	}

	pq.lock.Lock()
	defer pq.lock.Unlock()

	msg, err := pq.unflightMessage(msgId)
	if err != nil {
		return common.NewRetDataError(err)
	}

	msg.UnlockTs = util.Uts() + int64(timeout)

	pq.inFlightHeap.PushItem(msgId, msg.UnlockTs)
	pq.database.UpdateItem(pq.queueName, msg)

	return common.RETDATA_201OK
}

// Delete locked message by id.
func (pq *PQueue) DeleteLockedById(params map[string]string) *common.ReturnData {
	msgId, ok := params[defs.PRM_ID]
	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_ID_NOT_DEFINED)
	}

	pq.lock.Lock()
	defer pq.lock.Unlock()

	_, err := pq.unflightMessage(msgId)
	if err != nil {
		return common.NewRetDataError(err)
	}
	pq.deleteMessage(msgId)
	return common.RETDATA_201OK
}

func (pq *PQueue) UnlockMessageById(params map[string]string) *common.ReturnData {
	msgId, ok := params[defs.PRM_ID]
	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_ID_NOT_DEFINED)
	}

	pq.lock.Lock()
	defer pq.lock.Unlock()

	// Make sure message exists.
	msg, err := pq.unflightMessage(msgId)
	if err != nil {
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
			return qerrors.ERR_MSG_POP_ATTEMPTS_EXCEEDED
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
	iter := pq.database.IterQueue(pq.queueName)
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

var _ common.IItemHandler = &PQueue{}
