package mpqtesting

import (
	. "github.com/smartystreets/goconvey/convey"
	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/resp"
)

func VerifyItemsRespSize(r apis.IResponse, size int) ([]apis.IResponseItem, bool) {
	ir, ok := r.(*resp.MessagesResponse)
	So(ok, ShouldBeTrue)
	if ok {
		items := ir.GetItems()
		So(len(items), ShouldEqual, size)
		return items, len(items) == size
	}
	return nil, false
}

func VerifySingleItem(r apis.IResponse, itemId, payload string) bool {

	if items, ok := VerifyItemsRespSize(r, 1); ok {
		So(items[0].GetId(), ShouldEqual, itemId)
		So(string(items[0].GetPayload()), ShouldEqual, payload)
		return items[0].GetId() == itemId && string(items[0].GetPayload()) == payload
	}
	return false
}

func VerifyItems(r apis.IResponse, size int, itemSpecs ...string) bool {
	So(size*2, ShouldEqual, len(itemSpecs))
	items, ok := VerifyItemsRespSize(r, size)
	if size*2 == len(itemSpecs) && ok {
		for i := 0; i < len(itemSpecs); i += 2 {
			itemPos := i / 2
			itemId := itemSpecs[i]
			itemPayload := itemSpecs[i+1]
			So(items[itemPos].GetId(), ShouldEqual, itemId)
			So(string(items[itemPos].GetPayload()), ShouldEqual, itemPayload)
		}
		return true
	}
	return false
}

func VerifyOkResponse(r apis.IResponse) {
	So(r, ShouldEqual, resp.OK_RESPONSE)
}

func VerifyServiceSize(s apis.ISvc, size int) bool {
	So(s.GetSize(), ShouldEqual, size)
	return s.GetSize() == size
}
