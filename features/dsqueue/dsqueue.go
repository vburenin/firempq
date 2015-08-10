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
	"sort"
	"time"
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
	QUEUE_DIRECTION_NONE  = 0
	QUEUE_DIRECTION_FRONT = 1
	QUEUE_DIRECTION_BACK  = 2
	QUEUE_DIRECTION_FRONT_UNLOCKED = 3
	QUEUE_DIRECTION_BACK_UNLOCKED  = 4
)

const (
	TIMEOUT_MSG_DELIVERY = 1
	TIMEOUT_MSG_EXPIRATION = 2
)

const CLEAN_BATCH_SIZE = 1000000

type DSQueue struct {
	queueName string
	// Messages which are waiting to be picked up
	availableMsgs *structs.IndexList

	// Returned messages front/back. PopFront/PopBack first should pop elements from this lists
	highPriorityFrontMsgs *structs.IndexList
	highPriorityBackMsgs *structs.IndexList

	// All messages with the ticking counters except those which are inFlight.
	expireHeap *structs.IndexHeap
	// All locked messages and messages with delivery interval > 0
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
}

func initDSQueue(database *db.DataStorage, queueName string, settings *DSQueueSettings) *DSQueue {
	dsq := DSQueue{
		allMessagesMap:     	make(map[string]*DSQMessage),
		availableMsgs:      	structs.NewListQueue(),
		highPriorityFrontMsgs: 	structs.NewListQueue(),
		highPriorityBackMsgs: 	structs.NewListQueue(),
		database:           	database,
		expireHeap:         	structs.NewIndexHeap(),
		inFlightHeap:       	structs.NewIndexHeap(),
		queueName:          	queueName,
		settings:           	settings,
		workDone:           	false,
		newMsgNotification: 	make(chan bool),
	}

	dsq.actionHandlers = map[string](common.CallFuncType){
		ACTION_DELETE_LOCKED_BY_ID: dsq.DeleteLockedById,
		ACTION_DELETE_BY_ID:        dsq.DeleteById,
		ACTION_FORCE_DELETE_BY_ID:  dsq.ForceDelete,
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
	queue := initDSQueue(database, queueName, settings)
	queue.database.SaveQueueSettings(queueName, settings)
	return queue
}

func LoadDSQueue(database *db.DataStorage, queueName string) (common.IItemHandler, error) {
	settings := new(DSQueueSettings)
	err := database.GetQueueSettings(settings, queueName)
	if err != nil {
		return nil, err
	}
	dsq := initDSQueue(database, queueName, settings)
	return dsq, nil
}

func (dsq *DSQueue) GetStatus() map[string]interface{} {
	res := dsq.settings.ToMap()
	res["TotalMessages"] = len(dsq.allMessagesMap)
	res["InFlightSize"] = dsq.inFlightHeap.Len()
	res["LastPushTs"] = dsq.stats.LastPushTs
	res["LastPopTs"] = dsq.stats.LastPopTs
	res["LastPushFrontTs"] = dsq.stats.LastPushFrontTs
	res["LastPopFrontTs"] = dsq.stats.LastPopFrontTs
	res["LastPushBackTs"] = dsq.stats.LastPushBackTs
	res["LastPopBackTs"] = dsq.stats.LastPopBackTs
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
			dsq.deleteMessageById(id)
		}
		dsq.lock.Unlock()
	}
	log.Debug("Clear: Removed %d messages.", total)
}

func (dsq *DSQueue) Close() {
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
	return dsq.push(params, QUEUE_DIRECTION_FRONT)
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (dsq *DSQueue) PushBack(msgData map[string]string) *common.ReturnData {
	return dsq.push(msgData, QUEUE_DIRECTION_BACK)
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq *DSQueue) PopLockFront(params map[string]string) *common.ReturnData {

	return dsq.popMessage(QUEUE_DIRECTION_FRONT, false)
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq *DSQueue) PopLockBack(params map[string]string) *common.ReturnData {
	return dsq.popMessage(QUEUE_DIRECTION_BACK, false)
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq *DSQueue) PopFront(params map[string]string) *common.ReturnData {

	return dsq.popMessage(QUEUE_DIRECTION_FRONT, true)
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq *DSQueue) PopBack(params map[string]string) *common.ReturnData {
	return dsq.popMessage(QUEUE_DIRECTION_BACK, true)
}

// Return already locked message to the front of the queue
func (dsq *DSQueue) ReturnFront(params map[string]string) *common.ReturnData {
	return dsq.returnMessageTo(params, QUEUE_DIRECTION_FRONT)
}

// Return already locked message to the back of the queue
func (dsq *DSQueue) ReturnBack(params map[string]string) *common.ReturnData {
	return dsq.returnMessageTo(params, QUEUE_DIRECTION_BACK)
}

// Delete non-locked message from the queue by message's ID
func (dsq *DSQueue) DeleteById(params map[string]string) *common.ReturnData {
	msgId, ok := params[defs.PRM_ID]
	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_ID_NOT_DEFINED)
	}
	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	msg, ok := dsq.allMessagesMap[msgId]
	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_NOT_EXIST)
	}

	if dsq.inFlightHeap.ContainsId(msgId) {
		if msg.DeliveryTs > 0 {
			return common.NewRetDataError(qerrors.ERR_MSG_NOT_DELIVERED)
		} else {
			return common.NewRetDataError(qerrors.ERR_MSG_IS_LOCKED)
		}
	}

	dsq.deleteMessage(msg)

	return common.RETDATA_201OK
}

// Delete message locked or unlocked from the queue by message's ID
func (dsq *DSQueue) ForceDelete(params map[string]string) *common.ReturnData {
	msgId, ok := params[defs.PRM_ID]
	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_ID_NOT_DEFINED)
	}
	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	if !dsq.deleteMessageById(msgId) {
		return common.NewRetDataError(qerrors.ERR_MSG_NOT_EXIST)
	}

	return common.RETDATA_201OK
}

// Set a user defined message lock timeout. Only locked message timeout can be set.
func (dsq *DSQueue) SetLockTimeout(params map[string]string) *common.ReturnData {
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
func (dsq *DSQueue) DeleteLockedById(params map[string]string) *common.ReturnData {
	msgId, ok := params[defs.PRM_ID]
	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_ID_NOT_DEFINED)
	}

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	msg, err := dsq.unflightMessage(msgId)
	if err != nil {
		return common.NewRetDataError(err)
	}
	dsq.deleteMessage(msg)
	return common.RETDATA_201OK
}

// Unlock locked message by id.
// Message will be returned to the front/back of the queue based on Pop operation type (pop front/back)
func (dsq *DSQueue) UnlockMessageById(params map[string]string) *common.ReturnData {
	msgId, ok := params[defs.PRM_ID]
	if !ok {
		return common.NewRetDataError(qerrors.ERR_MSG_ID_NOT_DEFINED)
	}

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	// Make sure message exists and locked.
	msg, err := dsq.unflightMessage(msgId)
	if err != nil {
		return common.NewRetDataError(err)
	}
	// Message exists and locked, push it into the front of the queue.
	err = dsq.returnTo(msg, msg.pushAt)
	if err != nil {
		return common.NewRetDataError(err)
	}
	return common.RETDATA_201OK
}

// *********************************************************************************************************
// Internal subroutines
//

func (dsq *DSQueue) addMessageToQueue(msg *DSQMessage) error {

	switch msg.pushAt {
	case QUEUE_DIRECTION_FRONT:
		msg.ListId = int64(dsq.availableMsgs.Len()) + 1
		dsq.availableMsgs.PushFront(msg.Id)
	case QUEUE_DIRECTION_BACK:
		dsq.availableMsgs.PushBack(msg.Id)
		msg.ListId = -int64(dsq.availableMsgs.Len() + 1)
	case QUEUE_DIRECTION_FRONT_UNLOCKED:
		dsq.highPriorityFrontMsgs.PushFront(msg.Id)
		msg.ListId = int64(dsq.highPriorityFrontMsgs.Len())
	case QUEUE_DIRECTION_BACK_UNLOCKED:
		msg.ListId = int64(dsq.highPriorityBackMsgs.Len())
		dsq.highPriorityBackMsgs.PushFront(msg.Id)
	default:
		log.Error("Error! Wrong pushAt value %d for message %s", msg.pushAt, msg.Id)
		return qerrors.ERR_QUEUE_INTERNAL_ERROR
	}
	msg.DeliveryTs = 0
	return nil
}

func (dsq *DSQueue) storeMessage(msg *DSQMessage, payload string) *common.ReturnData {
	if _, ok := dsq.allMessagesMap[msg.Id]; ok {
		return common.NewRetDataError(qerrors.ERR_ITEM_ALREADY_EXISTS)
	}
	queueLen := dsq.availableMsgs.Len()
	dsq.allMessagesMap[msg.Id] = msg
	dsq.trackExpiration(msg)
	if msg.DeliveryTs > 0 {
		dsq.inFlightHeap.PushItem(msg.Id, msg.DeliveryTs)
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
	if deliveryInterval < 0 || deliveryInterval > int64(defs.TIMEOUT_MAX_DELIVERY) ||
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
	case QUEUE_DIRECTION_FRONT:
		dsq.stats.LastPushFrontTs = pushTime
	case QUEUE_DIRECTION_BACK:
		dsq.stats.LastPushBackTs = pushTime
	}
	defer dsq.lock.Unlock()
	return dsq.storeMessage(msg, payload)
}

func (dsq *DSQueue) popMetaMessage(popFrom uint8, permanentPop bool) *DSQMessage {
	var msgId = ""

	if dsq.availableMsgs.Empty() && dsq.highPriorityFrontMsgs.Empty() && dsq.highPriorityBackMsgs.Empty() {
		return nil
	}
	returnTo := QUEUE_DIRECTION_NONE
	switch popFrom {
	case QUEUE_DIRECTION_FRONT:
		returnTo = QUEUE_DIRECTION_FRONT_UNLOCKED
		if dsq.highPriorityFrontMsgs.Len() > 0 {
			msgId = dsq.highPriorityFrontMsgs.PopBack()
		} else {
			msgId = dsq.availableMsgs.PopFront()
		}
	case QUEUE_DIRECTION_BACK:
		returnTo = QUEUE_DIRECTION_BACK_UNLOCKED
		if dsq.highPriorityBackMsgs.Len() > 0 {
			msgId = dsq.highPriorityBackMsgs.PopBack()
		} else {
			msgId = dsq.availableMsgs.PopBack()
		}
	default:
		return nil
	}
	msg, ok := dsq.allMessagesMap[msgId]
	if !ok {
		return nil
	}
	msg.ListId = 0;
	if !permanentPop {
		msg.pushAt = uint8(returnTo)
		dsq.lockMessage(msg)
	} else {
		msg.pushAt = QUEUE_DIRECTION_NONE
	}
	dsq.database.UpdateItem(dsq.queueName, msg)

	return msg
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq *DSQueue) popMessage(direction uint8, permanentPop bool) *common.ReturnData {

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	msg := dsq.popMetaMessage(direction, permanentPop)
	if msg == nil {
		return common.NewRetDataError(qerrors.ERR_QUEUE_EMPTY)
	}
	retMsg := NewMsgItem(msg, dsq.getPayload(msg.Id))
	if permanentPop {
		dsq.deleteMessageById(msg.Id)
	}
	return common.NewRetData("Ok", defs.CODE_200_OK, []common.IItem{retMsg})
}

func (dsq *DSQueue) returnMessageTo(params map[string]string, place uint8) *common.ReturnData {
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
func (dsq *DSQueue) getPayload(msgId string) string {
	return dsq.database.GetPayload(dsq.queueName, msgId)
}

func (dsq *DSQueue) lockMessage(msg *DSQMessage) {
	nowTs := util.Uts()
	dsq.stats.LastPopTs = nowTs

	msg.PopCount += 1
	msg.UnlockTs = nowTs + dsq.settings.PopLockTimeout
	dsq.expireHeap.PopById(msg.Id)
	dsq.inFlightHeap.PushItem(msg.Id, msg.UnlockTs)
}

// Remove message id from In Flight message heap.
func (dsq *DSQueue) unflightMessage(msgId string) (*DSQMessage, error) {
	msg, ok := dsq.allMessagesMap[msgId]
	if !ok {
		return nil, qerrors.ERR_MSG_NOT_EXIST
	} else if msg.DeliveryTs > 0 {
		return nil, qerrors.ERR_MSG_NOT_DELIVERED
	}

	hi := dsq.inFlightHeap.PopById(msgId)
	if hi == structs.EMPTY_HEAP_ITEM {
		return nil, qerrors.ERR_MSG_NOT_LOCKED
	}

	return msg, nil
}

// Delete message from the queue
func (dsq *DSQueue) deleteMessage(msg *DSQMessage) {
	if msg == nil {
		return
	}
	switch msg.pushAt {
	case QUEUE_DIRECTION_FRONT_UNLOCKED:
		dsq.highPriorityFrontMsgs.RemoveById(msg.Id)
	case QUEUE_DIRECTION_BACK_UNLOCKED:
		dsq.highPriorityBackMsgs.RemoveById(msg.Id)
	default:
		dsq.availableMsgs.RemoveById(msg.Id)
	}
	delete(dsq.allMessagesMap, msg.Id)
	dsq.database.DeleteItem(dsq.queueName, msg.Id)
	dsq.expireHeap.PopById(msg.Id)
	dsq.inFlightHeap.PopById(msg.Id)
}

// Delete message from the queue by Id
func (dsq *DSQueue) deleteMessageById(msgId string) bool {
	if msg, ok := dsq.allMessagesMap[msgId]; ok {
		dsq.deleteMessage(msg)
		return true
	}
	return false
}

// Adds message into expiration heap. Not thread safe!
func (dsq *DSQueue) trackExpiration(msg *DSQMessage) {
	ok := dsq.expireHeap.PushItem(msg.Id, msg.CreatedTs+int64(dsq.settings.MsgTTL))
	if !ok {
		log.Error("Error! Item already exists in the expire heap: %s", msg.Id)
	}
}

// Attempts to return a message into the queue.
// If a number of POP attempts has exceeded, message will be deleted.
func (dsq *DSQueue) returnTo(msg *DSQMessage, place uint8) error {
	lim := dsq.settings.PopCountLimit
	if lim > 0 {
		if lim <= msg.PopCount {
			dsq.deleteMessageById(msg.Id)
			return qerrors.ERR_MSG_POP_ATTEMPTS_EXCEEDED
		}
	}
	msg.pushAt = place
	msg.UnlockTs = 0
	dsq.addMessageToQueue(msg)
	dsq.trackExpiration(msg)
	dsq.database.UpdateItem(dsq.queueName, msg)
	return nil
}

// Finish message delivery to the queue.
func (dsq *DSQueue) completeDelivery(msg *DSQMessage) {
	dsq.addMessageToQueue(msg)
	dsq.database.UpdateItem(dsq.queueName, msg)
}

// Unlocks all items which exceeded their lock/delivery time.
func (dsq *DSQueue) releaseInFlight() (int, int) {
	cur_ts := util.Uts()
	ifHeap := dsq.inFlightHeap

	returned :=0
	delivered := 0
	iteration := 0
	for !(ifHeap.Empty()) && ifHeap.MinElement() <= cur_ts {
		iteration += 1
		hi := ifHeap.PopItem()
		msg := dsq.allMessagesMap[hi.Id]
		if msg.DeliveryTs <= cur_ts {
			dsq.completeDelivery(msg)
			delivered += 1
		} else if msg.UnlockTs <= cur_ts {
			dsq.returnTo(msg, msg.pushAt)
			returned += 1
		} else {
			log.Error("Error! Wrong delivery/unlock interval specified for msg: %s", msg.Id)
		}
		if iteration >= MAX_CLEANS_PER_ATTEMPT {
			break
		}
	}
	return returned, delivered
}

// Remove all items which are completely expired.
func (dsq *DSQueue) cleanExpiredItems() int {
	cur_ts := util.Uts()
	eh := dsq.expireHeap

	i := 0
	for !(eh.Empty()) && eh.MinElement() < cur_ts {
		i++
		hi := eh.PopItem()
		dsq.deleteMessageById(hi.Id)
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
const DEFAULT_UNLOCK_INTERVAL = (10 * time.Millisecond) // 0.01 second

// Remove expired items. Should be running as a thread.
func (dsq *DSQueue) periodicCleanUp() {
	for !(dsq.workDone) {
		var sleepTime time.Duration = DEFAULT_UNLOCK_INTERVAL

		dsq.lock.Lock()
		if !(dsq.workDone) {
			unlocked, delivered := dsq.releaseInFlight()
			if delivered > 0 {
				log.Debug("%d messages delivered to the queue.", delivered)
				sleepTime = SLEEP_INTERVAL_IF_ITEMS
			}
			if unlocked > 0 {
				log.Debug("%d messages returned to the queue.", unlocked)
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
func (p MessageSlice) Less(i, j int) bool { return p[i].ListId < p[j].ListId }
func (p MessageSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (dsq *DSQueue) loadAllMessages() {
	nowTs := util.Uts()
	log.Info("Initializing queue: %s", dsq.queueName)
	iter := dsq.database.IterQueue(dsq.queueName)
	defer iter.Close()

	msgs := MessageSlice{}
	unlockedFrontMsgs := MessageSlice{}
 	unlockedBackMsgs := MessageSlice{}
 	delIds := []string{}

	s := dsq.settings
	for iter.Valid() {
		pqmsg := PQMessageFromBinary(string(iter.Key), iter.Value)

		// Store list if message IDs that should be removed.
		if pqmsg.CreatedTs+s.MsgTTL < nowTs ||
			(pqmsg.PopCount >= s.PopCountLimit && s.PopCountLimit > 0) {
			delIds = append(delIds, pqmsg.Id)
		} else {
			switch pqmsg.pushAt {
			case QUEUE_DIRECTION_FRONT_UNLOCKED:
				unlockedFrontMsgs = append(msgs, pqmsg)
			case QUEUE_DIRECTION_BACK_UNLOCKED:
				unlockedBackMsgs = append(msgs, pqmsg)
			default:
				msgs = append(msgs, pqmsg)
			}
		}
		iter.Next()
	}
	log.Debug("Loaded %d messages for %s queue",
		len(msgs) + len(unlockedFrontMsgs) + len(unlockedBackMsgs), dsq.queueName)
	if len(delIds) > 0 {
		log.Debug("%d messages will be removed because of expiration", len(delIds))
		for _, msgId := range delIds {
			dsq.database.DeleteItem(dsq.queueName, msgId)
		}

	}
	// Sorting data guarantees that messages will be available almost in the same order as they arrived.
	sort.Sort(unlockedFrontMsgs)
	for _, msg := range unlockedFrontMsgs {
		dsq.allMessagesMap[msg.Id] = msg
		dsq.highPriorityFrontMsgs.PushBack(msg.Id)
	}
	sort.Sort(unlockedBackMsgs)
	for _, msg := range unlockedBackMsgs {
		dsq.allMessagesMap[msg.Id] = msg
		dsq.highPriorityBackMsgs.PushBack(msg.Id)
	}
	sort.Sort(msgs)
	inDelivery := 0
	for _, msg := range msgs {
		dsq.allMessagesMap[msg.Id] = msg
		if msg.UnlockTs > nowTs {
			dsq.inFlightHeap.PushItem(msg.Id, msg.UnlockTs)
		} else {
			if 	msg.DeliveryTs > 0 {
				dsq.inFlightHeap.PushItem(msg.Id, msg.DeliveryTs)
				inDelivery += 1
			} else {
				dsq.expireHeap.PushItem(msg.Id, msg.CreatedTs+s.MsgTTL)
				dsq.availableMsgs.PushBack(msg.Id)
			}
		}
	}

	log.Debug("Messages available: %d", dsq.expireHeap.Len())
	log.Debug("Messages in flight: %d", dsq.inFlightHeap.Len())
	log.Debug("Messages in delivery: %d", inDelivery)
	log.Debug("Messages in high prioity/unlocked front: %d", dsq.highPriorityFrontMsgs.Len())
	log.Debug("Messages in high prioity/unlocked back: %d", dsq.highPriorityBackMsgs.Len())
}

var _ common.IItemHandler = &DSQueue{}
