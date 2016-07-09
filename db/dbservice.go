package db

import "github.com/vburenin/firempq/apis"

type DBService struct {
	database      apis.DataStorage
	itemPrefix    string
	payloadPrefix string
}

func (d *DBService) InitServiceDB(serviceId string) {
	d.itemPrefix = MakeItemPrefix(serviceId)
	d.payloadPrefix = MakePayloadPrefix(serviceId)
	d.database = DatabaseInstance()
}

// MakeItemPrefix makes a prefix which will be used to identify to which service item belongs to.
func MakeItemPrefix(serviceId string) string {
	return serviceId + "\x01"
}

// MakePayloadPrefix makes a prefix which will be used to identify to which service payload belongs to.
func MakePayloadPrefix(serviceId string) string {
	return serviceId + "\x02"
}

// CacheItemData stores only message metadata in the database.
func (d *DBService) CacheItemData(itemId string, itemData []byte) {
	d.database.CachedStore(d.itemPrefix+itemId, itemData)
}

// Payload returns message payload.
func (d *DBService) Payload(itemId string) []byte {
	return d.database.GetData(d.payloadPrefix + itemId)
}

// CacheAllItemData stores messages data and payload data into database.
func (d *DBService) CacheAllItemData(itemId string, metaData, payload []byte) {
	itemKey := d.itemPrefix + itemId
	payloadKey := d.payloadPrefix + itemId
	d.database.CachedStore2(itemKey, metaData, payloadKey, payload)
}

// DeleteAllItemData removes item from database including its payload.
func (d *DBService) DeleteAllItemData(itemId string) {
	d.database.DeleteCacheData(d.itemPrefix+itemId, d.payloadPrefix+itemId)
}

// ItemIterator returns an iterator over items which are matching provided prefix.
func (d *DBService) ItemIterator() apis.ItemIterator {
	return d.database.IterData(d.itemPrefix)
}

func (d *DBService) WaitFlush() {
	d.database.WaitFlush()
}
