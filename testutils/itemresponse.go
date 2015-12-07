package testutils

import (
	"firempq/common"

	. "firempq/api"

	. "github.com/smartystreets/goconvey/convey"
)

func VerifyItemsResponse(r IResponse, length int) ([]IItem, bool) {
	ir, ok := r.(*common.ItemsResponse)
	So(ok, ShouldBeTrue)
	if ok {
		items := ir.GetItems()
		So(len(items), ShouldEqual, length)
		return items, len(items) == length
	}
	return nil, false
}

func VerifyItem(r IResponse, itemId, payload string) bool {

	if items, ok := VerifyItemsResponse(r, 1); ok {
		So(items[0].GetId(), ShouldEqual, itemId)
		So(items[0].GetPayload(), ShouldEqual, payload)
		return items[0].GetId() == itemId && items[0].GetPayload() == payload
	}
	return false
}

func VerifyOk(r IResponse) {
	So(r, ShouldEqual, common.OK_RESPONSE)
}

func VerifyError(r IResponse) bool {
	err := r.IsError()
	So(err, ShouldBeTrue)
	return err
}

func VerifySize(s ISvc, size int) bool {
	So(s.Size(), ShouldEqual, size)
	return s.Size() == size
}
