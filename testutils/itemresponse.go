package testutils

import (
	"firempq/common"

	. "firempq/api"

	. "github.com/smartystreets/goconvey/convey"
)

func VerifyItemsResponse(r IResponse, size int) ([]IResponseItem, bool) {
	ir, ok := r.(*common.ItemsResponse)
	So(ok, ShouldBeTrue)
	if ok {
		items := ir.GetItems()
		So(len(items), ShouldEqual, size)
		return items, len(items) == size
	}
	return nil, false
}

func VerifySingleItem(r IResponse, itemId, payload string) bool {

	if items, ok := VerifyItemsResponse(r, 1); ok {
		So(items[0].GetId(), ShouldEqual, itemId)
		So(items[0].GetPayload(), ShouldEqual, payload)
		return items[0].GetId() == itemId && items[0].GetPayload() == payload
	}
	return false
}

func VerifyItems(r IResponse, size int, itemSpecs ...string) bool {
	So(size*2, ShouldEqual, len(itemSpecs))
	items, ok := VerifyItemsResponse(r, size)
	if size*2 == len(itemSpecs) && ok {
		for i := 0; i < len(itemSpecs); i += 2 {
			itemPos := i / 2
			itemId := itemSpecs[i]
			itemPayload := itemSpecs[i+1]
			So(items[itemPos].GetId(), ShouldEqual, itemId)
			So(items[itemPos].GetPayload(), ShouldEqual, itemPayload)
		}
		return true
	}
	return false
}

func VerifyOkResponse(r IResponse) {
	So(r, ShouldEqual, common.OK_RESPONSE)
}

func VerifyErrResponse(r IResponse) bool {
	err := r.IsError()
	So(err, ShouldBeTrue)
	return err
}

func VerifyServiceSize(s ISvc, size int) bool {
	So(s.GetSize(), ShouldEqual, size)
	return s.GetSize() == size
}
