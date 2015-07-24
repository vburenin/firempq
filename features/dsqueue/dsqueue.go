package dsqueue

import (
	"firempq/common"
	"firempq/db"
	"firempq/defs"
	"firempq/qerrors"
	"firempq/structs"
	"firempq/util"
	"github.com/op/go-logging"
	"strconv"
	"sync"
	"time"
	"testing"
)

var log = logging.MustGetLogger("firempq")

const (
	ACTION_UNLOCK_BY_ID        = "UNLOCK"
	ACTION_DELETE_LOCKED_BY_ID = "DELLOCKED"
	ACTION_DELETE_BY_ID        = "DEL"
	ACTION_SET_LOCK_TIMEOUT    = "SETLOCKTIMEOUT"
	ACTION_PUSH_FRONT          = "PUSHFRONT"
	ACTION_RETURN_FRONT        = "RETURNFRONT"
	ACTION_POP_FRONT           = "POPFRONT"
	ACTION_POP_LOCK_FRONT      = "POPLOCK"
	ACTION_PUSH_BACK           = "PUSHBACK"
	ACTION_RETURN_BACK         = "RETURNBACK"
	ACTION_POP_BACK            = "POPBACK"
	ACTION_POP_LOCK_BACK       = "POPLOCKBACK"
	ACTION_FORCE_DELETE_BY_ID  = "FORCEDELETE"
	ACTION_SET_QPARAM          = "SETQP"
	ACTION_SET_MPARAM          = "SETMP"
)

const (
	QEUE_DIRECTION_NONE = 0
	QEUE_DIRECTION_FRONT = 1
	QEUE_DIRECTION_BACK = 2
)

const CLEAN_BATCH_SIZE = 1000000

type DSQueue struct {
	queueName string
	// Messages which are waiting to be picked up
	availableMsgs *structs.IndexList

	// All messages with the ticking counters except those which are inFlight.
	expireHeap *structs.IndexHeap
	// All messages with delivery interval greater then 0
	deliveryHeap *structs.IndexHeap
	// All locked messages
	inFlightHeap *structs.IndexHeap
	// Just a message map message id to the full message data.
	allMessagesMap map[string]*DSQMessage

	lock sync.Mutex
	// Used to stop internal goroutine.
	workDone bool
	// Action handlers which are out of standard queue interface.
	actionHandlers map[string](common.CallFuncType)
	// Instance of the database.
	database *db.DataStorage
	// Queue settings.
	settings *DSQueueSettings
	// Queue statistics
	stats DSQueueStats

	newMsgNotification chan bool

	testLog *testing.T
}

func initFBueue(database *db.DataStorage, queueName string, settings *DSQueueSettings) *DSQueue {
	dsq := DSQueue{
		allMessagesMap:     make(map[string]*DSQMessage),
		availableMsgs:      structs.NewListQueue(),
		database:           database,
		expireHeap:         structs.NewIndexHeap(),
		inFlightHeap:       structs.NewIndexHeap(),
		deliveryHeap:       structs.NewIndexHeap(),
		queueName:          queueName,
		settings:           settings,
		workDone:           false,
		newMsgNotification: make(chan bool),
	}

	dsq.actionHandlers = map[string](common.CallFuncType){
		ACTION_DELETE_LOCKED_BY_ID: dsq.DeleteLockedById,
		ACTION_DELETE_BY_ID: 		dsq.DeleteById,
		ACTION_FORCE_DELETE_BY_ID:	dsq.ForceDelete,
		ACTION_SET_LOCK_TIMEOUT:    dsq.SetLockTimeout,
		ACTION_UNLOCK_BY_ID:        dsq.UnlockMessageById,
		ACTION_PUSH_FRONT:          dsq.PushFront,
		ACTION_POP_LOCK_FRONT:      dsq.PopLockFront,
		ACTION_POP_FRONT:           dsq.PopFront,
		ACTION_RETURN_FRONT:        dsq.ReturnFront,
		ACTION_PUSH_BACK:           dsq.PushBack,
		ACTION_POP_LOCK_BACK:       dsq.PopLockBack,
		ACTION_POP_BACK:            dsq.PopBack,
		ACTION_RETURN_BACK:         dsq.ReturnBack,
	}

	dsq.loadAllMessages()
	go dsq.periodicCleanUp()

	return &dsq
}

func NewDSQueue(database *db.DataStorage, queueName string, size int64) *DSQueue {
	settings := NewDSQueueSettings(size)
	queue := initFBueue(database, queueName, settings)
	queue.database.SaveQueueSettings(queueName, settings)
	return queue
}

func LoadDSQueue(database *db.DataStorage, queueName string) (common.IItemHandler, error) {
	settings := new(DSQueueSettings)
	err := database.GetQueueSettings(settings, queueName)
	if err != nil {
		return nil, err
	}
	dsq := initFBueue(database, queueName, settings)
	return dsq, nil
}

func (dsq *DSQueue) SetTestLog(testLog *testing.T) {
	dsq.testLog = testLog
}

func (dsq *DSQueue) GetStatus() map[string]interface{} {
	res := dsq.settings.ToMap()
	res["TotalMessages"] = len(dsq.allMessagesMap)
	res["InFlightSize"] = dsq.inFlightHeap.Len()
	return res
}

func (dsq *DSQueue) GetType() defs.ItemHandlerType {
	return defs.HT_DOUBLE_SIDED_QUEUE
}

// Queue custom specific handler for the queue type specific features.
func (dsq *DSQueue) Call(action string, params map[string]string) *common.ReturnData {
	handler, ok := dsq.actionHandlers[action]
	if !ok {
		return common.NewRetDataError(qerrors.InvalidRequest("Unknown action: " + action))
	}
	return handler(params)
}

// Delete all messages in the queue. It includes all type of messages
func (dsq *DSQueue) Clear() {
	total := 0
	for {
		ids := []string{}
		dsq.lock.Lock()
		if len(dsq.allMessagesMap) == 0 {
			dsq.lock.Unlock()
			break
		}
		for k, _ := range dsq.allMessagesMap {
			ids = append(ids, k)
			if len(ids) > CLEAN_BATCH_SIZE {
				break
			}
		}
		total += len(ids)
		for _, id := range ids {
			dsq.deleteMessage(id)
		}
		dsq.lock.Unlock()
	}
	log.Debug("Removed %d messages.", total)
}

func (dsq*DSQueue) Close() {
	dsq.lock.Lock()
	if dsq.workDone {
		return
	}
	dsq.workDone = true
	dsq.lock.Unlock()
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (dsq *DSQueue) PushFront(params map[string]string) *common.ReturnData {
	return dsq.push(params, QEUE_DIRECTION_FRONT)
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (dsq *DSQueue) PushBack(msgData map[string]string) *common.ReturnData {
	return dsq.push(msgData, QEUE_DIRECTION_BACK)
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq*DSQueue) PopLockFront(params map[string]string) *common.ReturnData {

	return dsq.popMessage(QEUE_DIRECTION_FRONT, false)
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq*DSQueue) PopLockBack(params map[string]string) *common.ReturnData {
	return dsq.popMessage(QEUE_DIRECTION_BACK, false)
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq*DSQueue) PopFront(params map[string]string) *common.ReturnData {

	return dsq.popMessage(QEUE_DIRECTION_FRONT, true)
}

func (dsq *DSQueue) ReturnFront(params map[string]string) *common.ReturnData {
	return dsq.returnMessageTo(params, QEUE_DIRECTION_FRONT)
}

func (dsq *DSQueue) ReturnBack(params map[string]string) *common.ReturnData {
	return dsq.returnMessageTo(params, QEUE_DIRECTION_BACK)
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq*DSQueue) PopBack(params map[string]string) *common.ReturnData {
	return dsq.popMessage(QEUE_DIRECTION_BACK, true)
}

func (dsq*DSQueue) DeleteById(params map[string]string) *common.ReturnData {
	msgId, ok := params[defs.PRM_ID]
	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_ID_NOT_DEFINED)
	}
	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	if dsq.inFlightHeap.ContainsId(msgId) {
		return common.NewRetDataError(qerrors.ERR_MSG_IS_LOCKED)
	}

	if !dsq.deleteMessage(msgId) {
		return common.NewRetDataError(qerrors.ERR_MSG_NOT_EXIST)
	}

	return common.RETDATA_201OK
}

func (dsq*DSQueue) ForceDelete(params map[string]string) *common.ReturnData {
	msgId, ok := params[defs.PRM_ID]
	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_ID_NOT_DEFINED)
	}
	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	dsq.unflightMessage(msgId)
	if !dsq.deleteMessage(msgId) {
		return common.NewRetDataError(qerrors.ERR_MSG_NOT_EXIST)
	}

	return common.RETDATA_201OK
}

// Set a user defined message lock timeout. Only locked message timeout can be set.
func (dsq*DSQueue) SetLockTimeout(params map[string]string) *common.ReturnData {
	msgId, ok := params[defs.PRM_ID]
	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_ID_NOT_DEFINED)
	}

	timeoutStr, ok := params[defs.PRM_TIMEOUT]

	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_TIMEOUT_NOT_DEFINED)
	}

	timeout, terr := strconv.Atoi(timeoutStr)
	if terr != nil || timeout < 0 || time.Duration(timeout) > defs.TIMEOUT_MAX_LOCK {
		return common.NewRetDataError(qerrors.ERR_MSG_TIMEOUT_IS_WRONG)
	}

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	msg, err := dsq.unflightMessage(msgId)
	if err != nil {
		return common.NewRetDataError(err)
	}

	msg.UnlockTs = util.Uts() + int64(timeout)

	dsq.inFlightHeap.PushItem(msgId, msg.UnlockTs)
	dsq.database.UpdateItem(dsq.queueName, msg)

	return common.RETDATA_201OK
}

// Delete locked message by id.
func (dsq*DSQueue) DeleteLockedById(params map[string]string) *common.ReturnData {
	msgId, ok := params[defs.PRM_ID]
	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_ID_NOT_DEFINED)
	}

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	_, err := dsq.unflightMessage(msgId)
	if err != nil {
		return common.NewRetDataError(err)
	}
	dsq.deleteMessage(msgId)
	return common.RETDATA_201OK
}

func (dsq*DSQueue) UnlockMessageById(params map[string]string) *common.ReturnData {
	msgId, ok := params[defs.PRM_ID]
	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_ID_NOT_DEFINED)
	}

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	// Make sure message exists.
	msg, err := dsq.unflightMessage(msgId)
	if err != nil {
		return common.NewRetDataError(err)
	}
	// Message exists, push it into the front of the queue.
	err = dsq.returnTo(msg, msg.pushAt)
	if err != nil {
		return common.NewRetDataError(err)
	}
	return common.RETDATA_201OK
}

//
// Internal subroutines
//

func (dsq *DSQueue) addMessageToQueue(msg *DSQMessage) {

	switch msg.pushAt {
		case QEUE_DIRECTION_FRONT:
			dsq.availableMsgs.PushFront(msg.Id)
		case QEUE_DIRECTION_BACK:
			dsq.availableMsgs.PushBack(msg.Id)
	}
}

func (dsq *DSQueue) storeMessage(msg *DSQMessage, payload string) *common.ReturnData {
	if _, ok := dsq.allMessagesMap[msg.Id]; ok {
		return common.NewRetDataError(qerrors.ERR_ITEM_ALREADY_EXISTS)
	}
	queueLen := dsq.availableMsgs.Len()
	dsq.allMessagesMap[msg.Id] = msg
	dsq.trackExpiration(msg)
	if msg.DeliveryTs > 0 {
		dsq.deliveryHeap.PushItem(msg.Id, msg.DeliveryTs)
	} else {
		dsq.addMessageToQueue(msg)
		if 0 == queueLen {
			select {
				case dsq.newMsgNotification <- true:
				default: // allows non blocking channel usage if there are no users awaiting for the message
			}
		}
		dsq.database.StoreItemWithPayload(dsq.queueName, msg, payload)
	}
	return common.RETDATA_201OK
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (dsq *DSQueue) push(msgData map[string]string, direction uint8) *common.ReturnData {

	msg, err := MessageFromMap(msgData)
	if err != nil {
		return common.NewRetDataError(err)
	}

	deliveryInterval := dsq.settings.DeliveryDelay
	// Optional delivery interval
	deliveryIntervalStr, ok := msgData[defs.PRM_DELIVERY_INTERVAL]
	if ok {
		var terr error = nil
		deliveryInterval, terr = strconv.ParseInt(deliveryIntervalStr, 10, 0)
		if terr != nil {
			return common.NewRetDataError(qerrors.ERR_MSG_DELIVERY_INTERVAL_NOT_DEFINED)
		}
	}
	if deliveryInterval < 0 || deliveryInterval > int64(defs.TIMEOUT_MAX_DELIVERY)  ||
	dsq.settings.MsgTTL < int64(deliveryInterval) {
		return common.NewRetDataError(qerrors.ERR_MSG_BAD_DELIVERY_TIMEOUT)
	}
	if deliveryInterval > 0 {
		msg.DeliveryTs = util.Uts() + deliveryInterval
	}

	payload, ok := msgData[defs.PRM_PAYLOAD]
	if !ok {
		payload = ""
	}
	msg.pushAt = direction

	dsq.lock.Lock()
	// Update stats
	pushTime := util.Uts()
	dsq.stats.LastPushTs = pushTime
	switch msg.pushAt {
		case QEUE_DIRECTION_FRONT:
			dsq.stats.LastPushFrontTs = pushTime
		case QEUE_DIRECTION_BACK:
			dsq.stats.LastPushBackTs = pushTime
	}
	defer dsq.lock.Unlock()
	return dsq.storeMessage(msg, payload)
}

func (dsq *DSQueue) popMetaMessage(popFrom uint8, permanentPop bool) *DSQMessage {
	var msgId = ""

	if dsq.availableMsgs.Empty() {
		return nil
	}

	switch popFrom {
		case QEUE_DIRECTION_FRONT:
			msgId = dsq.availableMsgs.PopFront()
		case QEUE_DIRECTION_BACK:
			msgId = dsq.availableMsgs.PopBack()
		default:
			return nil
	}
	msg, ok := dsq.allMessagesMap[msgId]
	if !ok {
		return nil
	}
	msg.PopCount += 1
	if !permanentPop {
		msg.pushAt = popFrom
		dsq.lockMessage(msg)
	} else {
		msg.pushAt = QEUE_DIRECTION_NONE
	}
	dsq.database.UpdateItem(dsq.queueName, msg)

	return msg
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq*DSQueue) popMessage(direction uint8, permanentPop bool) *common.ReturnData {

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	msg := dsq.popMetaMessage(direction, permanentPop)
	if msg == nil {
		return common.NewRetDataError(qerrors.ERR_QUEUE_EMPTY)
	}
	retMsg := NewMsgItem(msg, dsq.getPayload(msg.Id))
	if permanentPop {
		dsq.deleteMessage(msg.Id)
	}
	return common.NewRetData("Ok", defs.CODE_200_OK, []common.IItem{retMsg})
}

func (dsq*DSQueue) returnMessageTo(params map[string]string, place uint8) *common.ReturnData {
	msgId, ok := params[defs.PRM_ID]
	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_ID_NOT_DEFINED)
	}

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	// Make sure message exists.
	msg, err := dsq.unflightMessage(msgId)
	if err != nil {
		return common.NewRetDataError(err)
	}
	// Message exists, push it to the queue.
	err = dsq.returnTo(msg, place)
	if err != nil {
		return common.NewRetDataError(err)
	}
	return common.RETDATA_201OK
}

// Returns message payload.
// This method doesn't lock anything. Actually lock happens withing loadPayload structure.
func (dsq*DSQueue) getPayload(msgId string) string {
	return dsq.database.GetPayload(dsq.queueName, msgId)
}

func (dsq*DSQueue) lockMessage(msg *DSQMessage) {
	nowTs := util.Uts()
	dsq.stats.LastPopTs = nowTs
	// Increase number of pop attempts.

	msg.UnlockTs = nowTs + dsq.settings.PopLockTimeout
	dsq.expireHeap.PopById(msg.Id)
	dsq.inFlightHeap.PushItem(msg.Id, msg.UnlockTs)
}

// Remove message id from In Flight message heap.
func (dsq*DSQueue) unflightMessage(msgId string) (*DSQMessage, error) {
	msg, ok := dsq.allMessagesMap[msgId]
	if !ok {
		return nil, qerrors.ERR_MSG_NOT_EXIST
	}

	hi := dsq.inFlightHeap.PopById(msgId)
	if hi == structs.EMPTY_HEAP_ITEM {
		return nil, qerrors.ERR_MSG_NOT_LOCKED
	}

	return msg, nil
}

func (dsq*DSQueue) deleteMessage(msgId string) bool {
	if _, ok := dsq.allMessagesMap[msgId]; ok {
		dsq.availableMsgs.RemoveById(msgId)
		delete(dsq.allMessagesMap, msgId)
		dsq.database.DeleteItem(dsq.queueName, msgId)
		dsq.expireHeap.PopById(msgId)
		dsq.deliveryHeap.PopById(msgId)
		return true
	}
	return false
}

// Adds message into expiration heap. Not thread safe!
func (dsq*DSQueue) trackExpiration(msg *DSQMessage) {
	ok := dsq.expireHeap.PushItem(msg.Id, msg.CreatedTs+int64(dsq.settings.MsgTTL))
	if !ok {
		log.Error("Error! Item already exists in the expire heap: %s", msg.Id)
	}
}

// Attempts to return a message into the front of the queue.
// If a number of POP attempts has exceeded, message will be deleted.
func (dsq*DSQueue) returnTo(msg *DSQMessage, place uint8) error {
	lim := dsq.settings.PopCountLimit
	if lim > 0 {
		if lim <= msg.PopCount {
			dsq.deleteMessage(msg.Id)
			return qerrors.ERR_MSG_POP_ATTEMPTS_EXCEEDED
		}
	}
	switch place {
		case QEUE_DIRECTION_FRONT:
			dsq.availableMsgs.PushFront(msg.Id)
		case QEUE_DIRECTION_BACK:
			dsq.availableMsgs.PushFront(msg.Id)
		default:
			// Log & Return some error here
	}
	msg.UnlockTs = 0
	msg.pushAt = QEUE_DIRECTION_NONE;
	dsq.trackExpiration(msg)
	dsq.database.UpdateItem(dsq.queueName, msg)
	return nil
}

// Unlocks all items which exceeded their lock time.
func (dsq*DSQueue) releaseInFlight() int {
	cur_ts := util.Uts()
	ifHeap := dsq.inFlightHeap

	i := 0
	for !(ifHeap.Empty()) && ifHeap.MinElement() <= cur_ts  {
		i++
		hi := ifHeap.PopItem()
		pqmsg := dsq.allMessagesMap[hi.Id]
		dsq.returnTo(pqmsg, pqmsg.pushAt)
		if i >= MAX_CLEANS_PER_ATTEMPT {
			break
		}
	}
	return i
}

// Deliver postponed messages to a queue.
func (dsq *DSQueue) deliverMessages() int {
	cur_ts := util.Uts()
	dh := dsq.deliveryHeap
	i := 0
	for !(dh.Empty()) && dh.MinElement() < cur_ts {
		msg, ok := dsq.allMessagesMap[dh.PopItem().Id]
		if ok {
			dsq.addMessageToQueue(msg)
		}
		if i += 1; i >= MAX_DELIVERYS_PER_ATTEMPT {
			break
		}
	}
	return i
}

// Remove all items which are completely expired.
func (dsq*DSQueue) cleanExpiredItems() int {
	cur_ts := util.Uts()
	eh := dsq.expireHeap

	i := 0
	for !(eh.Empty()) && eh.MinElement() < cur_ts {
		i++
		hi := eh.PopItem()
		// There are two places to remove expired item:
		// 1. Map of all items - all items map.
		// 2. Available message.
		msg, ok := dsq.allMessagesMap[hi.Id]
		if ok {
			dsq.availableMsgs.RemoveById(msg.Id)
		}
		dsq.deleteMessage(msg.Id)
		if i >= MAX_CLEANS_PER_ATTEMPT {
			break
		}
	}
	return i
}

// 1 milliseconds.
const SLEEP_INTERVAL_IF_ITEMS = (1 * time.Millisecond)

// 1000 items should be enough to not create long locks. In the most good cases clean ups are rare.
const MAX_CLEANS_PER_ATTEMPT = 1000

// 1000 items should be enough to not create long locks. In the most good cases clean ups are rare.
const MAX_DELIVERYS_PER_ATTEMPT = 1000

// How frequently loop should run.
const DEFAULT_UNLOCK_INTERVAL = (10 * time.Millisecond)// 0.01 second

// Remove expired items. Should be running as a thread.
func (dsq*DSQueue) periodicCleanUp() {
	for !(dsq.workDone) {
		var sleepTime time.Duration = DEFAULT_UNLOCK_INTERVAL

		dsq.lock.Lock()
		if !(dsq.workDone) {
			cleaned := dsq.deliverMessages()
			if cleaned > 0 {
				log.Debug("%d messages pushed to the queue.", cleaned)
				sleepTime = SLEEP_INTERVAL_IF_ITEMS
			}
		}
		dsq.lock.Unlock()

		dsq.lock.Lock()
		if !(dsq.workDone) {
			cleaned := dsq.releaseInFlight()
			if cleaned > 0 {
				log.Debug("%d messages returned to the front of the queue.", cleaned)
				sleepTime = SLEEP_INTERVAL_IF_ITEMS
			}
		}
		dsq.lock.Unlock()

		dsq.lock.Lock()
		if !(dsq.workDone) {
			cleaned := dsq.cleanExpiredItems()
			if cleaned > 0 {
				log.Debug("%d items expired.", cleaned)
				sleepTime = SLEEP_INTERVAL_IF_ITEMS
			}
		}
		dsq.lock.Unlock()
		if !dsq.workDone {
			time.Sleep(sleepTime)
		}
	}
}

// Database related data management.
type MessageSlice []*DSQMessage

func (p MessageSlice) Len() int           { return len(p) }
func (p MessageSlice) Less(i, j int) bool { return p[i].CreatedTs < p[j].CreatedTs }
func (p MessageSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (dsq*DSQueue) loadAllMessages() {
}

var _ common.IItemHandler = &DSQueue{}
