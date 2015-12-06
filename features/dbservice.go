package features

import (
	"firempq/common"
	"firempq/db"

	. "firempq/iface"
)

type DBService struct {
	database      *db.DataStorage
	itemPrefix    string
	payloadPrefix string
}

func (d *DBService) InitServiceDB(serviceId string) {
	d.itemPrefix = MakeItemPrefix(serviceId)
	d.payloadPrefix = MakePayloadPrefix(serviceId)
	d.database = db.GetDatabase()
}

// MakeItemPrefix makes a prefix which will be used to identify to which service item belongs to.
func MakeItemPrefix(serviceId string) string {
	return serviceId + "\x01"
}

// MakePayloadPrefix makes a prefix which will be used to identify to which service payload belongs to.
func MakePayloadPrefix(serviceId string) string {
	return serviceId + "\x02"
}

func (d *DBService) StoreItemBodyInDB(item IItemMetaData) {
	key := d.itemPrefix + item.GetId()
	itemData, _ := item.Marshal()
	d.database.FastStoreData(key, itemData)
}

// Returns message payload.
func (d *DBService) GetPayloadFromDB(itemId string) string {
	payloadId := d.payloadPrefix + itemId
	return common.UnsafeBytesToString(d.database.GetData(payloadId))
}

// storeFullMessage Stores messages data and payload data into database.
func (d *DBService) StoreFullItemInDB(item IItemMetaData, payload string) {
	id := item.GetId()
	itemKey := d.itemPrefix + id
	payloadKey := d.payloadPrefix + id
	itemData, _ := item.Marshal()
	payloadData := common.UnsafeStringToBytes(payload)
	d.database.FastStoreData2(itemKey, itemData, payloadKey, payloadData)
}

func (d *DBService) DeleteItemFromDB(itemId string) {
	itemKey := d.itemPrefix + itemId
	payloadKey := d.payloadPrefix + itemId
	d.database.FastDeleteData2(itemKey, payloadKey)
}

func (d *DBService) GetItemIterator() *db.ItemIterator {
	return d.database.IterData(d.itemPrefix)
}
