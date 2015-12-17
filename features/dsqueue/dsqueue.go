package dsqueue

import (
	"firempq/common"
	"firempq/conf"
	"firempq/defs"
	"firempq/features"
	"firempq/log"
	"firempq/structs"
	"fmt"
	"sort"
	"sync"
	"time"

	. "firempq/api"
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
	features.DBService
	// Messages which are waiting to be picked up
	availableMsgs *structs.IndexList

	// Returned messages front/back. PopFront/PopBack first should pop elements from this lists
	highPriorityFrontMsgs *structs.IndexList
	highPriorityBackMsgs  *structs.IndexList

	// All messages with the ticking counters except those which are inFlight.
	expireHeap *structs.IndexedPriorityQueue
	// All locked messages and messages with delivery interval > 0
	inFlightHeap *structs.IndexedPriorityQueue
	// Just a message map message id to the full message data.
	allMessagesMap map[string]*DSQMetaMessage

	lock sync.Mutex

	closedState common.BoolFlag
	// Action handlers which are out of standard queue interface.
	actionHandlers map[string](common.CallFuncType)

	// Queue settings.
	conf *DSQConfig
	// A must attribute of each service containing all essential service information generated upon creation.
	desc *common.ServiceDescription

	newMsgNotification chan struct{}

	msgSerialNumber uint64
}

func CreateDSQueue(desc *common.ServiceDescription, params []string) ISvc {
	return NewDSQueue(desc, 1000)
}

func (dsq *DSQueue) NewContext() ServiceContext {
	return &DSQContext{dsq, 0}
}

func initDSQueue(desc *common.ServiceDescription, conf *DSQConfig) *DSQueue {
	dsq := DSQueue{
		desc:                  desc,
		allMessagesMap:        make(map[string]*DSQMetaMessage),
		availableMsgs:         structs.NewListQueue(),
		highPriorityFrontMsgs: structs.NewListQueue(),
		highPriorityBackMsgs:  structs.NewListQueue(),
		expireHeap:            structs.NewIndexedPriorityQueue(),
		inFlightHeap:          structs.NewIndexedPriorityQueue(),
		conf:                  conf,
		newMsgNotification:    make(chan struct{}),
		msgSerialNumber:       0,
	}

	dsq.InitServiceDB(desc.ServiceId)
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
	features.SaveServiceConfig(desc.ServiceId, conf)
	return queue
}

func LoadDSQueue(desc *common.ServiceDescription) (ISvc, error) {
	conf := &DSQConfig{}
	err := features.LoadServiceConfig(desc.ServiceId, conf)
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
	res["TotalMessages"] = dsq.Size()
	res["InFlightSize"] = dsq.inFlightHeap.Len()
	res["LastPushTs"] = dsq.conf.LastPushTs
	res["LastPopTs"] = dsq.conf.LastPopTs
	return res
}

func (dsq *DSQueue) Size() int {
	return len(dsq.allMessagesMap)
}

func (dsq *DSQueue) GetCurrentStatus() IResponse {
	return common.NewDictResponse(dsq.GetStatus())
}

func (dsq *DSQueue) GetType() defs.ServiceType {
	return defs.HT_DOUBLE_SIDED_QUEUE
}

func (dsq *DSQueue) GetTypeName() string {
	return common.STYPE_DOUBLE_SIDED_QUEUE
}

func (dsq *DSQueue) GetServiceId() string {
	return dsq.desc.ServiceId
}

const CLEAN_BATCH_SIZE = 1000

// Clear removes all locked and unlocked messages.
func (dsq *DSQueue) Clear() {
	total := 0
	for {
		ids := make([]string, 0, CLEAN_BATCH_SIZE)
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
		for _, id := range ids {
			dsq.deleteMessageById(id)
		}
		dsq.lock.Unlock()
		total += len(ids)
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

func (dsq *DSQueue) ExpireItems(cutOffTs int64) IResponse {
	return nil
}

func (dsq *DSQueue) ReleaseInFlight(cutOffTs int64) IResponse {
	return nil
}

// DeleteById deletes non-locked message from the queue by message's ID
func (dsq *DSQueue) DeleteById(msgId string) IResponse {
	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	msg, ok := dsq.allMessagesMap[msgId]
	if !ok {
		return common.ERR_MSG_NOT_FOUND
	}

	if dsq.inFlightHeap.ContainsId(msgId) {
		return common.ERR_MSG_IS_LOCKED
	}

	dsq.deleteMessage(msg)

	return common.OK_RESPONSE
}

// ForceDeleteById deletes message locked or unlocked from the queue by message's ID
func (dsq *DSQueue) ForceDeleteById(msgId string) IResponse {
	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	if !dsq.deleteMessageById(msgId) {
		return common.ERR_MSG_NOT_FOUND
	}

	return common.OK_RESPONSE
}

// SetLockTimeout set lock timeout for the message that already locked.
func (dsq *DSQueue) SetLockTimeout(msgId string, lockTimeout int64) IResponse {
	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	msg, err := dsq.unflightMessage(msgId)
	if err != nil {
		return err
	}

	msg.UnlockTs = common.Uts() + int64(lockTimeout)

	dsq.inFlightHeap.PushItem(msgId, msg.UnlockTs)
	dsq.StoreItemBodyInDB(msg)

	return common.OK_RESPONSE
}

// DeleteLockedById deletes locked message by id.
func (dsq *DSQueue) DeleteLockedById(msgId string) IResponse {
	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	msg, err := dsq.unflightMessage(msgId)
	if err != nil {
		return err
	}
	dsq.deleteMessage(msg)
	return common.OK_RESPONSE
}

// UnlockMessageById unlock locked message by id.
// Message will be returned to the front/back of the queue based on Pop operation type (pop front/back)
func (dsq *DSQueue) UnlockMessageById(msgId string) IResponse {
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

func (dsq *DSQueue) addMessageToQueue(msg *DSQMetaMessage) error {
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
	return nil
}

func (dsq *DSQueue) storeMessage(msg *DSQMetaMessage, payload string) IResponse {
	if _, ok := dsq.allMessagesMap[msg.Id]; ok {
		return common.ERR_ITEM_ALREADY_EXISTS
	}
	dsq.allMessagesMap[msg.Id] = msg
	dsq.trackExpiration(msg)
	if msg.UnlockTs > 0 {
		dsq.inFlightHeap.PushItem(msg.Id, msg.UnlockTs)
	} else {
		dsq.addMessageToQueue(msg)
	}
	dsq.StoreFullItemInDB(msg, payload)
	common.NewMessageNotify(dsq.newMsgNotification)
	return common.OK_RESPONSE
}

// Push message to the queue.
// Pushing message automatically enables auto expiration.
func (dsq *DSQueue) Push(msgId string, payload string, deliveryDelay int64, direction int32) IResponse {

	dsq.conf.LastPushTs = common.Uts()

	msg := NewDSQMetaMessage(msgId)
	if deliveryDelay > 0 {
		msg.UnlockTs = common.Uts() + deliveryDelay
	}
	msg.PushAt = direction

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	dsq.msgSerialNumber += 1
	msg.SerialNumber = dsq.msgSerialNumber

	return dsq.storeMessage(msg, payload)
}

func (dsq *DSQueue) popMetaMessage(popFrom int32, permanentPop bool) *DSQMetaMessage {
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
	dsq.StoreItemBodyInDB(msg)

	return msg
}

// Pop first available message.
// Will return nil if there are no messages available.
func (dsq *DSQueue) popMessage(direction int32, permanentPop bool) IResponse {

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	msg := dsq.popMetaMessage(direction, permanentPop)
	if msg == nil {
		return common.NewItemsResponse([]IItem{})
	}
	retMsg := NewMsgItem(msg.Id, dsq.GetPayloadFromDB(msg.Id))
	if permanentPop {
		dsq.deleteMessageById(msg.Id)
	}
	return common.NewItemsResponse([]IItem{retMsg})
}

func (dsq *DSQueue) returnMessageTo(msgId string, place int32) IResponse {
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

func (dsq *DSQueue) lockMessage(msg *DSQMetaMessage) {
	nowTs := common.Uts()
	dsq.conf.LastPopTs = nowTs

	msg.PopCount += 1
	msg.UnlockTs = nowTs + dsq.conf.PopLockTimeout
	dsq.expireHeap.PopById(msg.Id)
	dsq.inFlightHeap.PushItem(msg.Id, msg.UnlockTs)
}

// Remove message id from In Flight message heap.
func (dsq *DSQueue) unflightMessage(msgId string) (*DSQMetaMessage, *common.ErrorResponse) {
	msg, ok := dsq.allMessagesMap[msgId]
	if !ok {
		return nil, common.ERR_MSG_NOT_FOUND
	}

	hi := dsq.inFlightHeap.PopById(msgId)
	if hi == structs.EmptyHeapItem {
		return nil, common.ERR_MSG_NOT_LOCKED
	}

	return msg, nil
}

// Delete message from the queue
func (dsq *DSQueue) deleteMessage(msg *DSQMetaMessage) {
	switch msg.PushAt {
	case QUEUE_DIRECTION_FRONT_UNLOCKED:
		dsq.highPriorityFrontMsgs.RemoveById(msg.Id)
	case QUEUE_DIRECTION_BACK_UNLOCKED:
		dsq.highPriorityBackMsgs.RemoveById(msg.Id)
	default:
		dsq.availableMsgs.RemoveById(msg.Id)
	}
	delete(dsq.allMessagesMap, msg.Id)
	dsq.expireHeap.PopById(msg.Id)
	dsq.inFlightHeap.PopById(msg.Id)
	dsq.DeleteItemFromDB(msg.Id)
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
func (dsq *DSQueue) trackExpiration(msg *DSQMetaMessage) {
	ok := dsq.expireHeap.PushItem(msg.Id, msg.CreatedTs+int64(dsq.conf.MsgTtl))
	if !ok {
		log.Error("Error! Item already exists in the expire heap: %s", msg.Id)
	}
}

// Attempts to return a message into the queue.
// If a number of POP attempts has exceeded, message will be deleted.
func (dsq *DSQueue) returnTo(msg *DSQMetaMessage, place int32) *common.ErrorResponse {
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
	dsq.StoreItemBodyInDB(msg)
	common.NewMessageNotify(dsq.newMsgNotification)
	return nil
}

// Finish message delivery to the queue.
func (dsq *DSQueue) completeDelivery(msg *DSQMetaMessage) {
	dsq.addMessageToQueue(msg)
	dsq.StoreItemBodyInDB(msg)
	common.NewMessageNotify(dsq.newMsgNotification)
}

// Unlocks all items which exceeded their lock/delivery time.
func (dsq *DSQueue) releaseInFlight(ts int64) int64 {
	ifHeap := dsq.inFlightHeap
	var i int64 = 0
	var returned int64 = 0
	var delivered int64 = 0

	bs := conf.CFG.DSQueueConfig.UnlockBatchSize

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

	for !(ifHeap.Empty()) && ifHeap.MinElement() <= ts && i < bs {
		i += 1
		hi := ifHeap.PopItem()
		msg := dsq.allMessagesMap[hi.Id]
		if msg.PopCount == 0 {
			dsq.completeDelivery(msg)
			delivered += 1
		} else if msg.UnlockTs <= ts {
			dsq.returnTo(msg, msg.PushAt)
			returned += 1
		} else {
			log.Error("Error! Wrong delivery/unlock interval specified for msg: %s", msg.Id)
		}
	}
	return returned + delivered
}

// Remove all items which are completely expired.
func (dsq *DSQueue) cleanExpiredItems(ts int64) int64 {
	eh := dsq.expireHeap

	var i int64 = 0
	bs := conf.CFG.DSQueueConfig.UnlockBatchSize

	dsq.lock.Lock()
	defer dsq.lock.Unlock()

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
	go func() {
		for dsq.closedState.IsFalse() {
			if dsq.update(common.Uts()) {
				time.Sleep(time.Millisecond)
			} else {
				time.Sleep(conf.CFG.UpdateInterval * 1000)
			}
		}
	}()
}

// Remove expired items. Should be running as a thread.
func (dsq *DSQueue) update(ts int64) bool {
	var delivered, unlocked, cleaned int64

	released := dsq.releaseInFlight(ts)

	if released > 0 {
		log.Debug("Releasing %d messages back to the queue %s.", unlocked, dsq.desc.Name)
	}

	cleaned = dsq.cleanExpiredItems(ts)

	if cleaned > 0 {
		log.Debug("%d items expired.", cleaned)
	}

	return delivered+released > 0
}

// MessageSlice a list of messages to sort them.
type MessageSlice []*DSQMetaMessage

func (p MessageSlice) Len() int           { return len(p) }
func (p MessageSlice) Less(i, j int) bool { return p[i].SerialNumber < p[j].SerialNumber }
func (p MessageSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (dsq *DSQueue) loadAllMessages() {
	nowTs := common.Uts()
	log.Info("Initializing queue: %s", dsq.desc.Name)
	iter := dsq.GetItemIterator()

	msgs := MessageSlice{}
	unlockedFrontMsgs := MessageSlice{}
	unlockedBackMsgs := MessageSlice{}
	delIds := []string{}

	cfg := dsq.conf
	for ; iter.Valid(); iter.Next() {
		msgId := common.UnsafeBytesToString(iter.GetTrimKey())
		msg := UnmarshalDSQMetaMessage(msgId, iter.GetValue())
		if msg == nil {
			continue
		}
		// Store list if message IDs that should be removed.
		if msg.CreatedTs+cfg.MsgTtl < nowTs ||
			(msg.PopCount >= cfg.PopCountLimit && cfg.PopCountLimit > 0) {
			delIds = append(delIds, msg.Id)
		} else {
			switch msg.PushAt {
			case QUEUE_DIRECTION_FRONT_UNLOCKED:
				unlockedFrontMsgs = append(unlockedFrontMsgs, msg)
			case QUEUE_DIRECTION_BACK_UNLOCKED:
				unlockedBackMsgs = append(unlockedBackMsgs, msg)
			default:
				msgs = append(msgs, msg)
			}
		}
	}
	iter.Close()

	log.Debug("Loaded %d messages for %s queue",
		len(msgs)+len(unlockedFrontMsgs)+len(unlockedBackMsgs),
		dsq.desc.Name)

	if len(delIds) > 0 {
		log.Debug("%d messages are expired", len(delIds))
		for _, msgId := range delIds {
			dsq.DeleteItemFromDB(msgId)
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
	for _, msg := range msgs {
		dsq.allMessagesMap[msg.Id] = msg
		if msg.UnlockTs > nowTs {
			dsq.inFlightHeap.PushItem(msg.Id, msg.UnlockTs)
		} else {
			dsq.expireHeap.PushItem(msg.Id, msg.CreatedTs+cfg.MsgTtl)
			if msg.PushAt == QUEUE_DIRECTION_FRONT {
				dsq.availableMsgs.PushFront(msg.Id)
			} else {
				dsq.availableMsgs.PushBack(msg.Id)
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
	log.Debug("Messages in high prioity/unlocked front: %d", dsq.highPriorityFrontMsgs.Len())
	log.Debug("Messages in high prioity/unlocked back: %d", dsq.highPriorityBackMsgs.Len())
}

var _ ISvc = &DSQueue{}
