package db

import (
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const DBDir = "dbdir"

func TestPutGetData(t *testing.T) {
	Convey("Should save items on disk and read them", t, func() {
		Convey("Save and read two items", func() {
			os.RemoveAll(DBDir)
			d, err := NewDataStorage(DBDir)
			So(err, ShouldBeNil)

			d.FastStoreData2("id1", []byte("data1"), "id2", []byte("data2"))
			d.Close()

			d, err = NewDataStorage(DBDir)
			So(err, ShouldBeNil)
			data1 := d.GetData("id1")
			data2 := d.GetData("id2")
			So(data1, ShouldResemble, []byte("data1"))
			So(data2, ShouldResemble, []byte("data2"))

			total := d.DeleteDataWithPrefix("id")
			So(total, ShouldEqual, 2)

			data1 = d.GetData("id1")
			So(data1, ShouldBeNil)
			d.Close()
			os.RemoveAll(DBDir)
		})

		Convey("Save and read two slow", func() {
			os.RemoveAll(DBDir)
			d, err := NewDataStorage(DBDir)

			So(err, ShouldBeNil)
			d.StoreData("key1", []byte("keydata"))
			d.Close()

			d, err = NewDataStorage(DBDir)
			So(err, ShouldBeNil)
			data := d.GetData("key1")
			So(data, ShouldResemble, []byte("keydata"))

			d.FastDeleteData("key1")
			data = d.GetData("key1")
			So(data, ShouldBeNil)

			d.Close()

			d, err = NewDataStorage(DBDir)
			So(err, ShouldBeNil)

			data = d.GetData("key1")
			So(data, ShouldBeNil)

			d.Close()
			os.RemoveAll(DBDir)
		})

		Convey("Iterator should return trimmed and full keys", func() {
			os.RemoveAll(DBDir)
			d, err := NewDataStorage(DBDir)
			if err != nil {

			}
			So(err, ShouldBeNil)

			d.StoreData("key1", []byte("keydata"))
			d.StoreData("key2", []byte("keydata"))
			d.StoreData("key3", []byte("keydata"))
			d.StoreData("eee3", []byte("keydata"))

			i := d.IterData("key")
			So(i.Valid(), ShouldBeTrue)
			So(i.Key, ShouldResemble, []byte("key1"))
			So(i.TrimKey, ShouldResemble, []byte("1"))
			i.Next()

			So(i.Valid(), ShouldBeTrue)
			So(i.Key, ShouldResemble, []byte("key2"))
			So(i.TrimKey, ShouldResemble, []byte("2"))

			i.Next()
			So(i.Valid(), ShouldBeTrue)
			So(i.Key, ShouldResemble, []byte("key3"))
			So(i.TrimKey, ShouldResemble, []byte("3"))

			i.Next()
			So(i.Valid(), ShouldBeFalse)

			d.Close()
			os.RemoveAll(DBDir)
		})
	})
}
