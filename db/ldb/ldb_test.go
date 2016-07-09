package ldb

import (
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/vburenin/firempq/conf"
)

const DBDir = "dbdir"

func TestPutGetData(t *testing.T) {
	Convey("Should save items on disk and read them", t, func() {
		Convey("Save and read two items", func() {
			os.RemoveAll(DBDir)
			d, err := NewLevelDBStorage(DBDir, conf.CFG)
			So(err, ShouldBeNil)

			d.CachedStore2("id1", []byte("data1"), "id2", []byte("data2"))
			d.Close()

			d, err = NewLevelDBStorage(DBDir, conf.CFG)
			So(err, ShouldBeNil)
			data1 := d.GetData("id1")
			data2 := d.GetData("id2")
			So(string(data1), ShouldEqual, "data1")
			So(string(data2), ShouldEqual, "data2")

			total := d.DeleteDataWithPrefix("id")
			So(total, ShouldEqual, 2)

			data1 = d.GetData("id1")
			So(data1, ShouldBeNil)
			d.Close()
			os.RemoveAll(DBDir)
		})

		Convey("Save and read two slow", func() {
			os.RemoveAll(DBDir)
			d, err := NewLevelDBStorage(DBDir, conf.CFG)

			So(err, ShouldBeNil)
			d.StoreData("key1", []byte("keydata"))
			d.Close()

			d, err = NewLevelDBStorage(DBDir, conf.CFG)
			So(err, ShouldBeNil)
			data := d.GetData("key1")
			So(string(data), ShouldEqual, "keydata")

			d.DeleteCacheData("key1")
			data = d.GetData("key1")
			So(data, ShouldBeNil)

			d.Close()

			d, err = NewLevelDBStorage(DBDir, conf.CFG)
			So(err, ShouldBeNil)

			data = d.GetData("key1")
			So(data, ShouldBeNil)

			d.Close()
			os.RemoveAll(DBDir)
		})

		Convey("Iterator should return trimmed and full keys", func() {
			os.RemoveAll(DBDir)
			d, err := NewLevelDBStorage(DBDir, conf.CFG)
			if err != nil {

			}
			So(err, ShouldBeNil)

			d.StoreData("key1", []byte("keydata"))
			d.StoreData("key2", []byte("keydata"))
			d.StoreData("key3", []byte("keydata"))
			d.StoreData("eee3", []byte("keydata"))

			i := d.IterData("key")
			So(i.Valid(), ShouldBeTrue)
			So(i.GetKey(), ShouldResemble, []byte("key1"))
			So(i.GetTrimKey(), ShouldResemble, []byte("1"))
			i.Next()

			So(i.Valid(), ShouldBeTrue)
			So(i.GetKey(), ShouldResemble, []byte("key2"))
			So(i.GetTrimKey(), ShouldResemble, []byte("2"))

			i.Next()
			So(i.Valid(), ShouldBeTrue)
			So(i.GetKey(), ShouldResemble, []byte("key3"))
			So(i.GetTrimKey(), ShouldResemble, []byte("3"))

			i.Next()
			So(i.Valid(), ShouldBeFalse)

			d.Close()
			os.RemoveAll(DBDir)
		})
	})
}
