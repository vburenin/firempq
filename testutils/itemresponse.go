package testutils

import (
	"firempq/common"
	"firempq/iface"

	. "github.com/smartystreets/goconvey/convey"
)

func VerifyItemsResponse(r iface.IResponse, length int) ([]iface.IItem, bool) {
	ir, ok := r.(*common.ItemsResponse)
	So(ok, ShouldBeTrue)
	if ok {
		items := ir.GetItems()
		So(len(items), ShouldEqual, length)
		return items, len(items) == length
	}
	return nil, false
}

func VerifyItem(r iface.IResponse, itemId, payload string) bool {

	if items, ok := VerifyItemsResponse(r, 1); ok {
		So(items[0].GetId(), ShouldEqual, itemId)
		So(items[0].GetPayload(), ShouldEqual, payload)
		return items[0].GetId() == itemId && items[0].GetPayload() == payload
	}
	return false
}

func VerifyOk(r iface.IResponse) {
	So(r, ShouldEqual, common.OK_RESPONSE)
}

func VerifyError(r iface.IResponse) bool {
	err := r.IsError()
	So(err, ShouldBeTrue)
	return err
}

func VerifySize(s iface.ISvc, size int) bool {
	So(s.Size(), ShouldEqual, size)
	return s.Size() == size
}
