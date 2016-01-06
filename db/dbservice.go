package db

import (
	. "firempq/api"
)

type DBService struct {
	database      DataStorage
	itemPrefix    string
	payloadPrefix string
}

func (d *DBService) InitServiceDB(serviceId string) {
	d.itemPrefix = MakeItemPrefix(serviceId)
	d.payloadPrefix = MakePayloadPrefix(serviceId)
	d.database = GetDatabase()
}

// MakeItemPrefix makes a prefix which will be used to identify to which service item belongs to.
func MakeItemPrefix(serviceId string) string {
	return serviceId + "\x01"
}

// MakePayloadPrefix makes a prefix which will be used to identify to which service payload belongs to.
func MakePayloadPrefix(serviceId string) string {
	return serviceId + "\x02"
}

// StoreItemBodyInDB stores only message metadata in the database.
func (d *DBService) StoreItemBodyInDB(itemId, itemData string) {
	d.database.CachedStore(d.itemPrefix+itemId, itemData)
}

// GetPayloadFromDB returns message payload.
func (d *DBService) GetPayloadFromDB(itemId string) string {
	return d.database.GetData(d.payloadPrefix + itemId)
}

// StoreFullItemInDB stores messages data and payload data into database.
func (d *DBService) StoreFullItemInDB(itemId, itemData string, payload string) {
	itemKey := d.itemPrefix + itemId
	payloadKey := d.payloadPrefix + itemId
	d.database.CachedStore(itemKey, itemData, payloadKey, payload)
}

// DeleteFullItemFromDB removes item from database including its payload.
func (d *DBService) DeleteFullItemFromDB(itemId string) {
	d.database.CachedDeleteData(d.itemPrefix+itemId, d.payloadPrefix+itemId)
}

// GetItemIterator returns an iterator over items which are matching provided prefix.
func (d *DBService) GetItemIterator() ItemIterator {
	return d.database.IterData(d.itemPrefix)
}

func (d *DBService) WaitFlush() {
	d.database.WaitFlush()
}
