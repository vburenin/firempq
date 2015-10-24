package dsqueue

import (
	"firempq/common"
	"firempq/iface"
	"firempq/conf"
	"firempq/db"
	"firempq/defs"
	"firempq/log"
	"firempq/structs"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

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
	ACTION_STATUS              = "STATUS"
	ACTION_RELEASE_IN_FLIGHT   = "RELEASE"
	ACTION_EXPIRE              = "EXPIRE"
)

const (
	QUEUE_DIRECTION_NONE           = 0
	QUEUE_DIRECTION_FRONT          = 1
	QUEUE_DIRECTION_BACK           = 2
	QUEUE_DIRECTION_FRONT_UNLOCKED = 3
	QUEUE_DIRECTION_BACK_UNLOCKED  = 4
)

const (
	CONF_DEFAULT_TTL             = 10000 // In milliseconds
	CONF_DEFAULT_DELIVERY_DELAY  = 0
	CONF_DEFAULT_LOCK_TIMEOUT    = 1000 // In milliseconds
	CONF_DEFAULT_POP_COUNT_LIMIT = 0    // 0 means Unlimited.
)

type DSQueue struct {
	serviceId string
	// Messages which are waiting to be picked up
	availableMsgs *structs.IndexList

	// Returned messages front/back. PopFront/PopBack first should pop elements from this lists
	highPriorityFrontMsgs *structs.IndexList
	highPriorityBackMsgs  *structs.IndexList

	// All messages with the ticking counters except those which are inFlight.
	expireHeap *structs.IndexHeap
	// All locked messages and messages with delivery interval > 0
	inFlightHeap *structs.IndexHeap
	// Just a message map message id to the full message data.
	allMessagesMap map[string]*DSQMessage

	lock sync.Mutex

	closedState common.BoolFlag
	// Action handlers which are out of standard queue interface.
	actionHandlers map[string](common.CallFuncType)
	// Instance of the database.
	database *db.DataStorage

	// Queue settings.
	conf *DSQConfig
	// A must attribute of each service containing all essential service information generated upon creation.
	desc *common.ServiceDescription

	newMsgNotification chan bool

	msgSerialNumber uint64
}

func CreateDSQueue(desc *common.ServiceDescription, params []string) iface.ISvc {
	return NewDSQueue(desc, 1000)
}

func initDSQueue(desc *common.ServiceDescription, conf *DSQConfig) *DSQueue {
	dsq := DSQueue{
		desc:                  desc,
		allMessagesMap:        make(map[string]*DSQMessage),
		availableMsgs:         structs.NewListQueue(),
		highPriorityFrontMsgs: structs.NewListQueue(),
		highPriorityBackMsgs:  structs.NewListQueue(),
		database:              db.GetDatabase(),
		expireHeap:            structs.NewIndexHeap(),
		inFlightHeap:          structs.NewIndexHeap(),
		serviceId:             common.MakeServiceId(desc),
		conf:                  conf,
		newMsgNotification:    make(chan bool),
		msgSerialNumber:       0,
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
		ACTION_STATUS:              dsq.GetCurrentStatus,
		ACTION_RELEASE_IN_FLIGHT:   dsq.ReleaseInFlight,
		ACTION_EXPIRE:              dsq.ExpireItems,
	}

	dsq.loadAllMessages()

	return &dsq
}

func NewDSQueue(desc *common.ServiceDescription, size int64) *DSQueue {
	conf := &DSQConfig{
		MsgTtl:         int64(CONF_DEFAULT_TTL),
		DeliveryDelay:  int64(CONF_DEFAULT_DELIVERY_DELAY),
		PopLockTimeout: int64(CONF_DEFAULT_LOCK_TIMEOUT),
		PopCountLimit:  int64(CONF_DEFAULT_POP_COUNT_LIMIT),
		MaxSize:        size,
		LastPushTs:     common.Uts(),
		LastPopTs:      common.Uts(),
		InactivityTtl:  0,
	}
	queue := initDSQueue(desc, conf)
	queue.database.SaveServiceConfig(queue.serviceId, conf)
	return queue
}

func LoadDSQueue(desc *common.ServiceDescription) (iface.ISvc, error) {
	conf := &DSQConfig{}
	database := db.GetDatabase()
	err := database.LoadServiceConfig(conf, common.MakeServiceId(desc))
	if err != nil {
		return nil, err
	}
	dsq := initDSQueue(desc, conf)
	return dsq, nil
}

func (dsq *DSQueue) GetStatus() map[string]interface{} {
	res := make(map[string]interface{})
	res["MsgTtl"] = dsq.conf.GetMsgTtl()
	res["DeliveryDelay"] = dsq.conf.GetDeliveryDelay()
	res["PopLockTimeout"] = dsq.conf.GetPopLockTimeout()
	res["PopCountLimit"] = dsq.conf.GetPopCountLimit()
	res["MaxSize"] = dsq.conf.GetMaxSize()
	res["CreateTs"] = dsq.desc.GetCreateTs()
	res["TotalMessages"] = len(dsq.allMessagesMap)
	res["InFlightSize"] = dsq.inFlightHeap.Len()
	res["LastPushTs"] = dsq.conf.LastPushTs
	res["LastPopTs"] = dsq.conf.LastPopTs
	return res
}

func (dsq *DSQueue) GetCurrentStatus(params []string) iface.IResponse {
	if len(params) > 0 {
		return common.ERR_CMD_WITH_NO_PARAMS
	}
	return common.NewDictResponse(dsq.GetStatus())
}

func (dsq *DSQueue) GetType() defs.ServiceType {
	return defs.HT_DOUBLE_SIDED_QUEUE
}

func (dsq *DSQueue) GetTypeName() string {
	return common.STYPE_DOUBLE_SIDED_QUEUE
}

// Queue custom specific handler for the queue type specific features.
func (dsq *DSQueue) Call(action string, params []string) iface.IResponse {
	handler, ok := dsq.actionHandlers[action]
	if !ok {
		return common.InvalidRequest("Unknown action: " + action)
	}
	return handler(params)
}

const CLEAN_BATCH_SIZE = 1000

// Delete all messages in the queue. It includes all type of messages
func (dsq *DSQueue) Clear() {
	total := 0
	for {
		ids := make([]string, CLEAN_BATCH_SIZE)
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
	dsq.closedState.SetTrue()
}

func (dsq *DSQueue) IsClosed() bool {
	return dsq.closedState.IsTrue()
}

func makeUnknownParamResponse(paramName string) *common.ErrorResponse {
	return common.InvalidRequest(fmt.Sprintf("Unknown parameter: %s", paramName))
}

func getMessageIdOnly(params []string) (string, *common.ErrorResponse) {
	var err *common.ErrorResponse
	var msgId string

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = common.ParseStringParam(params, 1, 128)
		default:
			return "", makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return "", err
		}
	}

	if len(msgId) == 0 {
		return "", common.ERR_MSG_ID_NOT_DEFINED
	}
	return msgId, nil
}

func (dsq *DSQueue) tsParamFunc(params []string, f func(int64) int64) iface.IResponse {
	var err *common.ErrorResponse
	var ts int64 = -1
	for len(params) > 0 {
		switch params[0] {
		case PRM_TIMESTAMP:
			params, ts, err = common.ParseInt64Params(params, 0, math.MaxInt64)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return err
		}
	}
	if ts < 0 {
		return common.ERR_TS_PARAMETER_NEEDED
	}

	dsq.lock.Lock()
	defer dsq.lock.Unlock()
	if f == nil {
		return common.OK_RESPONSE
	}
	return common.NewIntResponse(f(ts))
}

func (dsq *DSQueue) ExpireItems(params []string) iface.IResponse {
	return dsq.tsParamFunc(params, nil)
}

func (dsq *DSQueue) ReleaseInFlight(params []string) iface.IResponse {
	return dsq.tsParamFunc(params, nil)
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (dsq *DSQueue) PushFront(params []string) iface.IResponse {
	return dsq.push(params, QUEUE_DIRECTION_FRONT)
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (dsq *DSQueue) PushBack(params []string) iface.IResponse {
	return dsq.push(params, QUEUE_DIRECTION_BACK)
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq *DSQueue) PopLockFront(params []string) iface.IResponse {
	return dsq.popMessage(QUEUE_DIRECTION_FRONT, false)
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq *DSQueue) PopLockBack(params []string) iface.IResponse {
	return dsq.popMessage(QUEUE_DIRECTION_BACK, false)
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq *DSQueue) PopFront(params []string) iface.IResponse {
	return dsq.popMessage(QUEUE_DIRECTION_FRONT, true)
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq *DSQueue) PopBack(params []string) iface.IResponse {
	return dsq.popMessage(QUEUE_DIRECTION_BACK, true)
}

// Return already locked message to the front of the queue
func (dsq *DSQueue) ReturnFront(params []string) iface.IResponse {
	return dsq.returnMessageTo(params, QUEUE_DIRECTION_FRONT)
}

// Return already locked message to the back of the queue
func (dsq *DSQueue) ReturnBack(params []string) iface.IResponse {
	return dsq.returnMessageTo(params, QUEUE_DIRECTION_BACK)
}

// Delete non-locked message from the queue by message's ID
func (dsq *DSQueue) DeleteById(params []string) iface.IResponse {
	msgId, retData := getMessageIdOnly(params)
	if retData != nil {
		return retData
	}
	msg, ok := dsq.allMessagesMap[msgId]
	if !ok {
		return common.ERR_MSG_NOT_EXIST
	}

	if dsq.inFlightHeap.ContainsId(msgId) {
		if msg.DeliveryTs > 0 {
			return common.ERR_MSG_NOT_DELIVERED
		} else {
			return common.ERR_MSG_IS_LOCKED
		}
	}

	dsq.deleteMessage(msg)

	return common.OK_RESPONSE
}

// Delete message locked or unlocked from the queue by message's ID
func (dsq *DSQueue) ForceDelete(params []string) iface.IResponse {
	msgId, retData := getMessageIdOnly(params)
	if retData != nil {
		return retData
	}

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	if !dsq.deleteMessageById(msgId) {
		return common.ERR_MSG_NOT_EXIST
	}

	return common.OK_RESPONSE
}

// Set a user defined message lock timeout. Only locked message timeout can be set.
func (dsq *DSQueue) SetLockTimeout(params []string) iface.IResponse {
	var err *common.ErrorResponse
	var msgId string
	var lockTimeout int64 = -1

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = common.ParseStringParam(params, 1, 128)
		case PRM_LOCK_TIMEOUT:
			params, lockTimeout, err = common.ParseInt64Params(params, 0, 24*1000*3600)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return err
		}
	}

	if len(msgId) == 0 {
		return common.ERR_MSG_ID_NOT_DEFINED
	}

	if lockTimeout < 0 {
		return common.ERR_MSG_TIMEOUT_NOT_DEFINED
	}

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	msg, err := dsq.unflightMessage(msgId)
	if err != nil {
		return err
	}

	msg.UnlockTs = common.Uts() + int64(lockTimeout)

	dsq.inFlightHeap.PushItem(msgId, msg.UnlockTs)
	dsq.database.StoreItem(dsq.serviceId, msg)

	return common.OK_RESPONSE
}

// Delete locked message by id.
func (dsq *DSQueue) DeleteLockedById(params []string) iface.IResponse {
	msgId, retData := getMessageIdOnly(params)
	if retData != nil {
		return retData
	}

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	msg, err := dsq.unflightMessage(msgId)
	if err != nil {
		return err
	}
	dsq.deleteMessage(msg)
	return common.OK_RESPONSE
}

// Unlock locked message by id.
// Message will be returned to the front/back of the queue based on Pop operation type (pop front/back)
func (dsq *DSQueue) UnlockMessageById(params []string) iface.IResponse {
	msgId, retData := getMessageIdOnly(params)
	if retData != nil {
		return retData
	}

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	// Make sure message exists and locked.
	msg, err := dsq.unflightMessage(msgId)
	if err != nil {
		return err
	}
	// Message exists and locked, push it into the front of the queue.
	err = dsq.returnTo(msg, msg.PushAt)
	if err != nil {
		return err
	}
	return common.OK_RESPONSE
}

// *********************************************************************************************************
// Internal subroutines
//

func (dsq *DSQueue) addMessageToQueue(msg *DSQMessage) error {

	switch msg.PushAt {
	case QUEUE_DIRECTION_FRONT:
		dsq.availableMsgs.PushFront(msg.Id)
	case QUEUE_DIRECTION_BACK:
		dsq.availableMsgs.PushBack(msg.Id)
	case QUEUE_DIRECTION_FRONT_UNLOCKED:
		dsq.highPriorityFrontMsgs.PushFront(msg.Id)
	case QUEUE_DIRECTION_BACK_UNLOCKED:
		dsq.highPriorityBackMsgs.PushFront(msg.Id)
	default:
		log.Error("Error! Wrong pushAt value %d for message %s", msg.PushAt, msg.Id)
		return common.ERR_QUEUE_INTERNAL_ERROR
	}
	msg.DeliveryTs = 0
	return nil
}

func (dsq *DSQueue) storeMessage(msg *DSQMessage, payload string) iface.IResponse {
	if _, ok := dsq.allMessagesMap[msg.Id]; ok {
		return common.ERR_ITEM_ALREADY_EXISTS
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
	}
	dsq.database.StoreItemWithPayload(dsq.serviceId, msg, payload)
	return common.OK_RESPONSE
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (dsq *DSQueue) push(params []string, direction int32) iface.IResponse {
	var err *common.ErrorResponse
	var msgId string
	var payload string = ""
	var deliveryDelay = dsq.conf.DeliveryDelay

	for len(params) > 0 {
		switch params[0] {
		case PRM_ID:
			params, msgId, err = common.ParseStringParam(params, 1, 128)
		case PRM_PAYLOAD:
			params, payload, err = common.ParseStringParam(params, 1, 512*1024)
		case PRM_DELAY:
			params, deliveryDelay, err = common.ParseInt64Params(params, 0, 3600*1000)
		default:
			return makeUnknownParamResponse(params[0])
		}
		if err != nil {
			return err
		}
	}
	if len(msgId) == 0 {
		msgId = common.GenRandMsgId()
	}

	if deliveryDelay < 0 || deliveryDelay > int64(defs.TIMEOUT_MAX_DELIVERY) ||
		dsq.conf.MsgTtl < deliveryDelay {
		return common.ERR_MSG_BAD_DELIVERY_TIMEOUT
	}

	dsq.conf.LastPushTs = common.Uts()

	msg := NewDSQMessage(msgId)
	if deliveryDelay > 0 {
		msg.DeliveryTs = common.Uts() + deliveryDelay
	}
	msg.PushAt = direction

	dsq.lock.Lock()
	dsq.msgSerialNumber += 1
	msg.SerialNumber = dsq.msgSerialNumber

	// Update stats
	defer dsq.lock.Unlock()

	return dsq.storeMessage(msg, payload)
}

func (dsq *DSQueue) popMetaMessage(popFrom int32, permanentPop bool) *DSQMessage {
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
	if !permanentPop {
		msg.PushAt = int32(returnTo)
		dsq.lockMessage(msg)
	} else {
		msg.PushAt = QUEUE_DIRECTION_NONE
	}
	dsq.database.StoreItem(dsq.serviceId, msg)

	return msg
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq *DSQueue) popMessage(direction int32, permanentPop bool) iface.IResponse {

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	msg := dsq.popMetaMessage(direction, permanentPop)
	if msg == nil {
		return common.NewItemsResponse([]iface.IItem{})
	}
	retMsg := NewMsgItem(msg, dsq.getPayload(msg.Id))
	if permanentPop {
		dsq.deleteMessageById(msg.Id)
	}
	return common.NewItemsResponse([]iface.IItem{retMsg})
}

func (dsq *DSQueue) returnMessageTo(params []string, place int32) iface.IResponse {
	msgId, retData := getMessageIdOnly(params)
	if retData != nil {
		return retData
	}

	dsq.lock.Lock()
	defer dsq.lock.Unlock()
	// Make sure message exists.
	msg, err := dsq.unflightMessage(msgId)
	if err != nil {
		return err
	}
	// Message exists, push it to the queue.
	err = dsq.returnTo(msg, place)
	if err != nil {
		return err
	}
	return common.OK_RESPONSE
}

// Returns message payload.
// This method doesn't lock anything. Actually lock happens withing loadPayload structure.
func (dsq *DSQueue) getPayload(msgId string) string {
	return dsq.database.GetPayload(dsq.serviceId, msgId)
}

func (dsq *DSQueue) lockMessage(msg *DSQMessage) {
	nowTs := common.Uts()
	dsq.conf.LastPopTs = nowTs

	msg.PopCount += 1
	msg.UnlockTs = nowTs + dsq.conf.PopLockTimeout
	dsq.expireHeap.PopById(msg.Id)
	dsq.inFlightHeap.PushItem(msg.Id, msg.UnlockTs)
}

// Remove message id from In Flight message heap.
func (dsq *DSQueue) unflightMessage(msgId string) (*DSQMessage, *common.ErrorResponse) {
	msg, ok := dsq.allMessagesMap[msgId]
	if !ok {
		return nil, common.ERR_MSG_NOT_EXIST
	} else if msg.DeliveryTs > 0 {
		return nil, common.ERR_MSG_NOT_DELIVERED
	}

	hi := dsq.inFlightHeap.PopById(msgId)
	if hi == structs.EMPTY_HEAP_ITEM {
		return nil, common.ERR_MSG_NOT_LOCKED
	}

	return msg, nil
}

// Delete message from the queue
func (dsq *DSQueue) deleteMessage(msg *DSQMessage) {
	if msg == nil {
		return
	}
	switch msg.PushAt {
	case QUEUE_DIRECTION_FRONT_UNLOCKED:
		dsq.highPriorityFrontMsgs.RemoveById(msg.Id)
	case QUEUE_DIRECTION_BACK_UNLOCKED:
		dsq.highPriorityBackMsgs.RemoveById(msg.Id)
	default:
		dsq.availableMsgs.RemoveById(msg.Id)
	}
	delete(dsq.allMessagesMap, msg.Id)
	dsq.database.DeleteItem(dsq.serviceId, msg.Id)
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
	ok := dsq.expireHeap.PushItem(msg.Id, msg.CreatedTs+int64(dsq.conf.MsgTtl))
	if !ok {
		log.Error("Error! Item already exists in the expire heap: %s", msg.Id)
	}
}

// Attempts to return a message into the queue.
// If a number of POP attempts has exceeded, message will be deleted.
func (dsq *DSQueue) returnTo(msg *DSQMessage, place int32) *common.ErrorResponse {
	lim := dsq.conf.PopCountLimit
	if lim > 0 {
		if lim <= msg.PopCount {
			dsq.deleteMessageById(msg.Id)
			return common.ERR_MSG_POP_ATTEMPTS_EXCEEDED
		}
	}
	msg.PushAt = place
	msg.UnlockTs = 0
	dsq.addMessageToQueue(msg)
	dsq.trackExpiration(msg)
	dsq.database.StoreItem(dsq.serviceId, msg)
	return nil
}

// Finish message delivery to the queue.
func (dsq *DSQueue) completeDelivery(msg *DSQMessage) {
	dsq.addMessageToQueue(msg)
	dsq.database.StoreItem(dsq.serviceId, msg)
}

// Unlocks all items which exceeded their lock/delivery time.
func (dsq *DSQueue) releaseInFlight(ts int64) (int64, int64) {
	ifHeap := dsq.inFlightHeap
	var i int64 = 0
	var returned int64 = 0
	var delivered int64 = 0

	bs := conf.CFG.DSQueueConfig.UnlockBatchSize
	for !(ifHeap.Empty()) && ifHeap.MinElement() <= ts && i < bs {
		i += 1
		hi := ifHeap.PopItem()
		msg := dsq.allMessagesMap[hi.Id]
		if msg.DeliveryTs <= ts {
			dsq.completeDelivery(msg)
			delivered += 1
		} else if msg.UnlockTs <= ts {
			dsq.returnTo(msg, msg.PushAt)
			returned += 1
		} else {
			log.Error("Error! Wrong delivery/unlock interval specified for msg: %s", msg.Id)
		}
	}
	return returned, delivered
}

// Remove all items which are completely expired.
func (dsq *DSQueue) cleanExpiredItems(ts int64) int64 {
	eh := dsq.expireHeap

	var i int64 = 0
	bs := conf.CFG.DSQueueConfig.UnlockBatchSize
	for !(eh.Empty()) && eh.MinElement() < ts && i < bs {
		i++
		hi := eh.PopItem()
		dsq.deleteMessageById(hi.Id)
		log.Debug("Deleting expired message: %s.", hi.Id)
	}
	return i
}

// StartUpdate runs a loop of periodic data updates.
func (dsq *DSQueue) StartUpdate() {
	go dsq.updateLoop()
}

func (dsq *DSQueue) updateLoop() {
	for dsq.closedState.IsFalse() {
		if dsq.update(common.Uts()) {
			time.Sleep(time.Millisecond)
		} else {
			time.Sleep(conf.CFG.UpdateInterval * 1000)
		}
	}
}

// Remove expired items. Should be running as a thread.
func (dsq *DSQueue) update(ts int64) bool {
	var delivered, unlocked, cleaned int64
	dsq.lock.Lock()
	unlocked, delivered = dsq.releaseInFlight(ts)
	dsq.lock.Unlock()

	if delivered > 0 {
		log.Debug("%d messages delivered to the queue %s.", delivered, dsq.serviceId)
	}
	if unlocked > 0 {
		log.Debug("%d messages returned to the queue %s.", unlocked, dsq.serviceId)
	}

	dsq.lock.Lock()
	cleaned = dsq.cleanExpiredItems(ts)
	dsq.lock.Unlock()

	if cleaned > 0 {
		log.Debug("%d items expired.", cleaned)
	}

	return delivered > 0 || cleaned > 0 || unlocked > 0
}

// Database related data management.
type MessageSlice []*DSQMessage

func (p MessageSlice) Len() int           { return len(p) }
func (p MessageSlice) Less(i, j int) bool { return p[i].SerialNumber < p[j].SerialNumber }
func (p MessageSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (dsq *DSQueue) loadAllMessages() {
	nowTs := common.Uts()
	log.Info("Initializing queue: %s", dsq.desc.Name)
	iter := dsq.database.IterServiceItems(dsq.serviceId)
	defer iter.Close()

	msgs := MessageSlice{}
	unlockedFrontMsgs := MessageSlice{}
	unlockedBackMsgs := MessageSlice{}
	delIds := []string{}

	cfg := dsq.conf
	for iter.Valid() {
		pqmsg := UnmarshalDSQMessage(string(iter.Key), iter.Value)
		// Store list if message IDs that should be removed.
		if pqmsg.CreatedTs+cfg.MsgTtl < nowTs ||
			(pqmsg.PopCount >= cfg.PopCountLimit && cfg.PopCountLimit > 0) {
			delIds = append(delIds, pqmsg.Id)
		} else {
			switch pqmsg.PushAt {
			case QUEUE_DIRECTION_FRONT_UNLOCKED:
				unlockedFrontMsgs = append(unlockedFrontMsgs, pqmsg)
			case QUEUE_DIRECTION_BACK_UNLOCKED:
				unlockedBackMsgs = append(unlockedBackMsgs, pqmsg)
			default:
				msgs = append(msgs, pqmsg)
			}
		}
		iter.Next()
	}
	log.Debug("Loaded %d messages for %s queue",
		len(msgs)+len(unlockedFrontMsgs)+len(unlockedBackMsgs),
		dsq.desc.Name)

	if len(delIds) > 0 {
		log.Debug("%d messages are expired", len(delIds))
		for _, msgId := range delIds {
			dsq.database.DeleteItem(dsq.serviceId, msgId)
		}
	}

	// Sorting data guarantees that messages will be available:w
	// almost in the same order as they arrived.
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
			if msg.DeliveryTs > nowTs {
				dsq.inFlightHeap.PushItem(msg.Id, msg.DeliveryTs)
				inDelivery += 1
			} else {
				dsq.expireHeap.PushItem(msg.Id, msg.CreatedTs+cfg.MsgTtl)
				dsq.availableMsgs.PushFront(msg.Id)
			}
		}
	}

	lastEl := len(msgs) - 1
	if lastEl >= 0 && msgs[lastEl].SerialNumber > dsq.msgSerialNumber {
		dsq.msgSerialNumber = msgs[lastEl].SerialNumber
	}

	lastEl = len(unlockedFrontMsgs) - 1
	if lastEl >= 0 && unlockedFrontMsgs[lastEl].SerialNumber > dsq.msgSerialNumber {
		dsq.msgSerialNumber = unlockedFrontMsgs[lastEl].SerialNumber
	}

	lastEl = len(unlockedBackMsgs) - 1
	if lastEl >= 0 && unlockedBackMsgs[lastEl].SerialNumber > dsq.msgSerialNumber {
		dsq.msgSerialNumber = unlockedBackMsgs[lastEl].SerialNumber
	}

	log.Debug("Messages available: %d", dsq.expireHeap.Len())
	log.Debug("Messages in flight: %d", dsq.inFlightHeap.Len())
	log.Debug("Messages in delivery (part of in flight): %d", inDelivery)
	log.Debug("Messages in high prioity/unlocked front: %d", dsq.highPriorityFrontMsgs.Len())
	log.Debug("Messages in high prioity/unlocked back: %d", dsq.highPriorityBackMsgs.Len())
}

var _ iface.ISvc = &DSQueue{}
