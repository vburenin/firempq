package pqueue

/*
import (
	"math"
	"strconv"
	"testing"
	"time"
	"sync"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/vburenin/firempq/apis"
	"github.com/vburenin/firempq/conf"
	"github.com/vburenin/firempq/db"
	"github.com/vburenin/firempq/log"
	"github.com/vburenin/firempq/mpqerr"
	"github.com/vburenin/firempq/mpqproto/resp"
	"github.com/vburenin/firempq/queue_info"
)

func getCtxConfig() *conf.PQConfig {
	return &conf.PQConfig{
		MaxMsgsInQueue: 100001,
		MaxMsgSize:     256000,
		MsgTtl:         100000,
		DeliveryDelay:  1,
		PopLockTimeout: 10000,
		PopCountLimit:  4,
		LastPushTs:     12,
		LastPopTs:      13,
	}
}

func getCtxDesc() *queue_info.ServiceDescription {
	return &queue_info.ServiceDescription{
		ExportId:  10,
		SType:     "PQueue",
		Name:      "name",
		CreateTs:  123,
		Disabled:  false,
		ToDelete:  false,
		ServiceId: "1",
	}
}

type FakeCtxSvcLoader struct{}

func (f *FakeCtxSvcLoader) GetService(name string) (apis.ISvc, bool) { return nil, false }

func i2a(v int64) string { return strconv.FormatInt(v, 10) }

func CreateQueueTestContext() (*ConnScope, *TestResponseWriter) {
	rw := NewTestResponseWriter()
	return NewPQueue(&FakeCtxSvcLoader{}, getCtxDesc(), getCtxConfig()).ConnScope(rw).(*ConnScope), rw
}

func CreateNewQueueTestContext() (*ConnScope, *TestResponseWriter) {
	log.InitLogging()
	log.SetLevel(1)
	db.SetDatabase(NewInMemDBService())
	return CreateQueueTestContext()
}

func TestCtxParsePQConfig(t *testing.T) {
	Convey("All config parameters should be parsed correctly", t, func() {
		Convey("Check all parameters are correct", func() {
			params := []string{
				CPRM_MSG_TTL, "100",
				CPRM_MAX_MSGS_IN_QUEUE, "200",
				CPRM_DELIVERY_DELAY, "300",
				CPRM_POP_LIMIT, "400",
				CPRM_LOCK_TIMEOUT, "500",
			}
			cfg, resp := ParsePQConfig(params)
			VerifyOkResponse(resp)
			So(cfg.MsgTtl, ShouldEqual, 100)
			So(cfg.MaxMsgsInQueue, ShouldEqual, 200)
			So(cfg.DeliveryDelay, ShouldEqual, 300)
			So(cfg.PopCountLimit, ShouldEqual, 400)
			So(cfg.PopLockTimeout, ShouldEqual, 500)
		})
		Convey("Message ttl parse error", func() {
			_, err := ParsePQConfig([]string{CPRM_MSG_TTL, "-1"})
			So(err.StringResponse(), ShouldContainSubstring, i2a(conf.CFG_PQ.MaxMessageTTL))
		})
		Convey("Max size parse error", func() {
			_, err := ParsePQConfig([]string{CPRM_MAX_MSGS_IN_QUEUE, "-1"})
			So(err.StringResponse(), ShouldContainSubstring, i2a(math.MaxInt64))
		})
		Convey("Delivery delay parse error", func() {
			_, err := ParsePQConfig([]string{CPRM_DELIVERY_DELAY, "-1"})
			So(err.StringResponse(), ShouldContainSubstring, i2a(conf.CFG_PQ.MaxDeliveryDelay))
		})
		Convey("Pop limit parse error", func() {
			_, err := ParsePQConfig([]string{CPRM_POP_LIMIT, "-1"})
			So(err.StringResponse(), ShouldContainSubstring, i2a(math.MaxInt64))
		})
		Convey("Lock timeout parse error", func() {
			_, err := ParsePQConfig([]string{CPRM_LOCK_TIMEOUT, "-1"})
			So(err.StringResponse(), ShouldContainSubstring, i2a(conf.CFG_PQ.MaxLockTimeout))
		})
	})
}

func TestCtxPopLock(t *testing.T) {
	Convey("Test POPLOCK command", t, func() {
		q, rw := CreateNewQueueTestContext()
		Convey("Lock timeout error should occure", func() {
			resp := q.Call(PQ_CMD_POPLOCK, []string{PrmLockTimeout, "-1"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(conf.CFG_PQ.MaxLockTimeout))
		})

		Convey("Limit error should occure", func() {
			resp := q.Call(PQ_CMD_POPLOCK, []string{PrmLimit, "-1"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(conf.CFG_PQ.MaxPopBatchSize))
		})

		Convey("Pop wait error should occure", func() {
			resp := q.Call(PQ_CMD_POPLOCK, []string{PrmPopWait, "-1"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(conf.CFG_PQ.MaxPopWaitTimeout))
		})

		Convey("Async error should occure", func() {
			resp := q.Call(PQ_CMD_POPLOCK, []string{PrmAsync, "--++--"})
			So(resp.StringResponse(), ShouldContainSubstring, "Only [_a-z")
		})

		Convey("Unknown param error should occure", func() {
			resp := q.Call(PQ_CMD_POPLOCK, []string{"PARAM_PAM", "--++--"})
			So(resp.StringResponse(), ShouldContainSubstring, "Unknown")
		})

		Convey("Async pop should return empty list", func() {
			resp := q.Call(PQ_CMD_POPLOCK, []string{PrmAsync, "a1", PrmPopWait, "1"})
			So(resp.StringResponse(), ShouldEqual, "+A a1")
			time.Sleep(time.Millisecond * 10)
			So(len(rw.GetResponses()), ShouldEqual, 1)
			So(rw.GetResponses()[0].StringResponse(), ShouldEqual, "+ASYNC a1 +MSGS *0")
		})

		Convey("Pop should return empty list", func() {
			p := []string{PrmPopWait, "1", PrmLockTimeout, "100", PrmLimit, "10"}
			resp := q.Call(PQ_CMD_POPLOCK, p)
			VerifyItems(resp, 0)
		})

	})
}

func TestCtxPop(t *testing.T) {
	Convey("Pop command should work", t, func() {
		q, rw := CreateNewQueueTestContext()

		Convey("Limit error should occure", func() {
			resp := q.Call(PQ_CMD_POP, []string{PrmLimit, "-1"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(conf.CFG_PQ.MaxPopBatchSize))
		})

		Convey("Pop wait error should occure", func() {
			resp := q.Call(PQ_CMD_POP, []string{PrmPopWait, "-1"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(conf.CFG_PQ.MaxPopWaitTimeout))
		})

		Convey("Async error should occure", func() {
			resp := q.Call(PQ_CMD_POP, []string{PrmAsync, "--++--"})
			So(resp.StringResponse(), ShouldContainSubstring, "Only [_a-z")
		})

		Convey("Unknown param error should occure", func() {
			resp := q.Call(PQ_CMD_POP, []string{"PARAM_PAM", "--++--"})
			So(resp.StringResponse(), ShouldContainSubstring, "Unknown")
		})

		Convey("Async pop should return empty list", func() {
			resp := q.Call(PQ_CMD_POP, []string{PrmAsync, "a1", PrmPopWait, "1"})
			So(resp.StringResponse(), ShouldEqual, "+A a1")
			time.Sleep(time.Millisecond * 10)
			So(len(rw.GetResponses()), ShouldEqual, 1)
			So(rw.GetResponses()[0].StringResponse(), ShouldEqual, "+ASYNC a1 +MSGS *0")
		})

		Convey("Pop async run error because POP WAIT is 0", func() {
			p := []string{PrmPopWait, "0", PrmLimit, "10", PrmAsync, "id1"}
			resp := q.Call(PQ_CMD_POP, p)
			So(resp.StringResponse(), ShouldContainSubstring, "+ASYNC id1 -ERR")
		})

		Convey("Pop should return empty list", func() {
			p := []string{PrmPopWait, "1", PrmLimit, "10"}
			resp := q.Call(PQ_CMD_POP, p)
			VerifyItems(resp, 0)
		})
	})
}

func TestCtxGetMessageInfo(t *testing.T) {
	Convey("No message info should be available", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("No params provided errors should be returned", func() {
			resp := q.Call(PQ_CMD_MSG_INFO, []string{})
			So(resp, ShouldEqual, mpqerr.ERR_MSG_ID_NOT_DEFINED)
		})

		Convey("Wrong message ID format should be detected", func() {
			resp := q.Call(PQ_CMD_MSG_INFO, []string{"$"})
			So(resp, ShouldEqual, mpqerr.ERR_ID_IS_WRONG)
		})

		Convey("Wrong message ID not found", func() {
			resp := q.Call(PQ_CMD_MSG_INFO, []string{"1234"})
			So(resp, ShouldEqual, mpqerr.ERR_MSG_NOT_FOUND)
		})
	})
}

func TestCtxDeleteLockedByID(t *testing.T) {
	Convey("Deleting locked messages by id should work", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("No params provided errors should be returned", func() {
			resp := q.Call(PQ_CMD_DELETE_LOCKED_BY_ID, []string{})
			So(resp, ShouldEqual, mpqerr.ERR_MSG_ID_NOT_DEFINED)
		})

		Convey("Wrong message ID format should be detected", func() {
			resp := q.Call(PQ_CMD_DELETE_LOCKED_BY_ID, []string{"$"})
			So(resp, ShouldEqual, mpqerr.ERR_ID_IS_WRONG)
		})

		Convey("Message is not locked error should be returned", func() {
			q.Call(PQ_CMD_PUSH, []string{PrmPayload, "t", PrmID, "id1", PrmDelay, "0"})
			resp := q.Call(PQ_CMD_DELETE_LOCKED_BY_ID, []string{"id1"})
			So(resp, ShouldEqual, mpqerr.ERR_MSG_NOT_LOCKED)
			VerifyServiceSize(q.pq, 1)
		})
	})
}

func TestCtxDeleteByID(t *testing.T) {
	Convey("Deleting message by id should work well", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("No params provided errors should be returned", func() {
			resp := q.Call(PQ_CMD_DELETE_BY_ID, []string{})
			So(resp, ShouldEqual, mpqerr.ERR_MSG_ID_NOT_DEFINED)
		})

		Convey("Wrong message ID format should be detected", func() {
			resp := q.Call(PQ_CMD_DELETE_BY_ID, []string{"$"})
			So(resp, ShouldEqual, mpqerr.ERR_ID_IS_WRONG)
		})

		Convey("Message should be deleted", func() {
			q.Call(PQ_CMD_PUSH, []string{PrmPayload, "t", PrmID, "id1", PrmDelay, "0"})
			resp := q.Call(PQ_CMD_DELETE_BY_ID, []string{"id1"})
			VerifyOkResponse(resp)
		})
	})
}

func TestCtxPush(t *testing.T) {
	Convey("Push command should work fine", t, func() {
		q, rw := CreateNewQueueTestContext()

		Convey("Should not accept messages with underscore prefix", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PrmID, "_ab", PrmPayload, "p"})
			So(resp, ShouldEqual, mpqerr.ERR_USER_ID_IS_WRONG)
		})

		Convey("Priority should be out of range", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PrmID, "ab", PrmPayload, "p", PRM_PRIORITY, "-1"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(math.MaxInt64))
		})

		Convey("Message ttl should error", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PrmID, "ab", PrmPayload, "p", PrmMsgTTL, "-1"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(conf.CFG_PQ.MaxMessageTTL))
		})

		Convey("Delivery delay must be out of range", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PrmID, "ab", PrmPayload, "p", PrmDelay, "-1"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(conf.CFG_PQ.MaxDeliveryDelay))
		})
		Convey("Push with sync wait. Push should succed. Nothing special will happen.", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PrmID, "ab", PrmPayload, "p", PrmDelay, "1", PrmSyncWait})
			VerifyOkResponse(resp)
			VerifyServiceSize(q.pq, 1)
		})
		Convey("Push async with no wait flag. Should fail with error", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PrmID, "ab", PrmPayload, "p", PrmDelay, "1", PrmAsync, "asid"})
			So(resp.StringResponse(), ShouldContainSubstring, "+ASYNC asid -ERR")
		})
		Convey("Push async with wait flag. Should succed with two responses.", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PrmID, "ab", PrmPayload, "p", PrmAsync, "asid", PrmSyncWait})
			So(resp.StringResponse(), ShouldContainSubstring, "+A asid")
			time.Sleep(time.Millisecond * 10)
			So(rw.GetResponses()[0].StringResponse(), ShouldEqual, "+ASYNC asid +OK")
			VerifyServiceSize(q.pq, 1)
		})
		Convey("Push with unknown param.", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PrmID, "ab", PrmPayload, "p", "TEST_PARAM"})
			So(resp.StringResponse(), ShouldContainSubstring, "TEST_PARAM")
		})
	})
}

func TestCtxUpdateLock(t *testing.T) {
	Convey("Update lock should work fine", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("Should fail with unknown param", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_ID, []string{PrmID, "ab", "TEST_PARAM"})
			So(resp.StringResponse(), ShouldContainSubstring, "TEST_PARAM")
		})
		Convey("Failure with incorrect message id", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_ID, []string{PrmID, "$ab", PrmLockTimeout, "10000"})
			So(resp, ShouldEqual, mpqerr.ERR_ID_IS_WRONG)
		})

		Convey("Failure with empty message id", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_ID, []string{PrmLockTimeout, "1"})
			So(resp, ShouldEqual, mpqerr.ERR_MSG_ID_NOT_DEFINED)
		})
		Convey("Failure with no timeout defined", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_ID, []string{PrmID, "1234"})
			So(resp, ShouldEqual, mpqerr.ERR_MSG_TIMEOUT_NOT_DEFINED)
		})

		Convey("Failure with no message", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_ID, []string{PrmID, "1234", PrmLockTimeout, "100"})
			So(resp, ShouldEqual, mpqerr.ERR_MSG_NOT_FOUND)
		})

		Convey("Failure with to wrong timeout", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_ID, []string{PrmID, "1234", PrmLockTimeout, "-1"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(conf.CFG_PQ.MaxLockTimeout))
		})
	})
}

func TestCtxUnlockMessageByID(t *testing.T) {
	Convey("Unlocking message should work as expected", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("No params provided errors should be returned", func() {
			resp := q.Call(PQ_CMD_UNLOCK_BY_ID, []string{})
			So(resp, ShouldEqual, mpqerr.ERR_MSG_ID_NOT_DEFINED)
		})

		Convey("Wrong message ID format should be detected", func() {
			resp := q.Call(PQ_CMD_UNLOCK_BY_ID, []string{"$"})
			So(resp, ShouldEqual, mpqerr.ERR_ID_IS_WRONG)
		})

		Convey("Message not found error should happend", func() {
			resp := q.Call(PQ_CMD_UNLOCK_BY_ID, []string{"id1"})
			So(resp, ShouldEqual, mpqerr.ERR_MSG_NOT_FOUND)
		})
	})
}

func TestCtxGetCurrentStatus(t *testing.T) {
	Convey("Get current status error should be returned", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("No params should be provided error should be returned", func() {
			resp := q.Call(PQ_CMD_STATUS, []string{"PRM"})
			So(resp, ShouldEqual, mpqerr.ERR_CMD_WITH_NO_PARAMS)
		})
		Convey("Should return service status", func() {
			_, ok := q.Call(PQ_CMD_STATUS, []string{}).(*resp.DictResponse)
			So(ok, ShouldBeTrue)
		})
	})
}

func TestCtxCheckTimeouts(t *testing.T) {
	Convey("Unlocking message should work as expected", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("TS param needed error should be returned", func() {
			resp := q.Call(PQ_CMD_CHECK_TIMEOUTS, []string{})
			So(resp, ShouldEqual, mpqerr.ERR_TS_PARAMETER_NEEDED)
		})
		Convey("Should return unknown param error", func() {
			resp := q.Call(PQ_CMD_CHECK_TIMEOUTS, []string{"TEST_PARAM"})
			So(resp.StringResponse(), ShouldContainSubstring, "TEST_PARAM")
		})
		Convey("Should return wrong TS error", func() {
			resp := q.Call(PQ_CMD_CHECK_TIMEOUTS, []string{PrmTimeStamp, "-1"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(math.MaxInt64))
		})
		Convey("Should work", func() {
			resp := q.Call(PQ_CMD_CHECK_TIMEOUTS, []string{PrmTimeStamp, "1000"})
			So(resp.StringResponse(), ShouldEqual, "+DATA :0")
		})
	})
}

func TestCtxSetParamValue(t *testing.T) {
	Convey("Set param should work well", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("At least one parameter should be provided error", func() {
			resp := q.Call(PQ_CMD_SET_CFG, []string{})
			So(resp, ShouldEqual, mpqerr.ERR_CMD_PARAM_NOT_PROVIDED)
		})
		Convey("Message TTL error", func() {
			resp := q.Call(PQ_CMD_SET_CFG, []string{CPRM_MSG_TTL, "0"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(conf.CFG_PQ.MaxMessageTTL))
		})
		Convey("Queue Max Size", func() {
			resp := q.Call(PQ_CMD_SET_CFG, []string{CPRM_MAX_MSGS_IN_QUEUE, "-1"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(math.MaxInt64))
		})
		Convey("Message delivery delay error", func() {
			resp := q.Call(PQ_CMD_SET_CFG, []string{CPRM_DELIVERY_DELAY, "-1"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(conf.CFG_PQ.MaxDeliveryDelay))
		})
		Convey("Pop limit out of range error", func() {
			resp := q.Call(PQ_CMD_SET_CFG, []string{CPRM_POP_LIMIT, "-1"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(math.MaxInt64))
		})

		Convey("Lock timeout error", func() {
			resp := q.Call(PQ_CMD_SET_CFG, []string{CPRM_LOCK_TIMEOUT, "-1"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(conf.CFG_PQ.MaxLockTimeout))
		})

		Convey("Should return unknown param error", func() {
			resp := q.Call(PQ_CMD_SET_CFG, []string{"TEST_PARAM"})
			So(resp.StringResponse(), ShouldContainSubstring, "TEST_PARAM")
		})
		Convey("All parameters should be set", func() {
			params := []string{
				CPRM_DELIVERY_DELAY, "100",
				CPRM_MSG_TTL, "10000",
				CPRM_MAX_MSGS_IN_QUEUE, "100000",
			}
			VerifyOkResponse(q.Call(PQ_CMD_SET_CFG, params))
		})
	})
}

func TestCtxDeleteByReceipt(t *testing.T) {
	Convey("All call scenarios should return expected response", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("Should return expired error", func() {
			resp := q.Call(PQ_CMD_DELETE_BY_RCPT, []string{"1-2"})
			So(resp, ShouldEqual, mpqerr.ERR_RECEIPT_EXPIRED)
		})
		Convey("Should return invalid receipt error", func() {
			resp := q.Call(PQ_CMD_DELETE_BY_RCPT, []string{"!@#$%%"})
			So(resp, ShouldEqual, mpqerr.ERR_INVALID_RECEIPT)
		})
		Convey("Should return no receipt provided error", func() {
			resp := q.Call(PQ_CMD_DELETE_BY_RCPT, []string{})
			So(resp, ShouldEqual, mpqerr.ERR_NO_RECEIPT)
		})
	})
}

func TestCtxUnlockByReceipt(t *testing.T) {
	Convey("All call scenarios should return expected response", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("Should return expired error", func() {
			resp := q.Call(PQ_CMD_UNLOCK_BY_RCPT, []string{"1-2"})
			So(resp, ShouldEqual, mpqerr.ERR_RECEIPT_EXPIRED)
		})
		Convey("Should return invalid receipt error", func() {
			resp := q.Call(PQ_CMD_UNLOCK_BY_RCPT, []string{"!@#$%%"})
			So(resp, ShouldEqual, mpqerr.ERR_INVALID_RECEIPT)
		})
		Convey("Should return no receipt provided error", func() {
			resp := q.Call(PQ_CMD_UNLOCK_BY_RCPT, []string{})
			So(resp, ShouldEqual, mpqerr.ERR_NO_RECEIPT)
		})
	})
}

func TestCtxUpdateLockByReceipt(t *testing.T) {
	Convey("All call scenarios should return expected response", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("Should return no timeout parameter error", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_RCPT, []string{PrmReceipt, "1-2"})
			So(resp, ShouldEqual, mpqerr.ERR_MSG_TIMEOUT_NOT_DEFINED)
		})
		Convey("Should return invalid timeout error", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_RCPT, []string{PrmReceipt, "1-2", PrmLockTimeout, "-1"})
			So(resp.StringResponse(), ShouldContainSubstring, i2a(conf.CFG_PQ.MaxLockTimeout))
		})
		Convey("Should return unknown parameter error", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_RCPT, []string{"UNKNOWN"})
			So(resp.StringResponse(), ShouldContainSubstring, "UNKNOWN")
		})
	})
}

func TestCtxFinish(t *testing.T) {
	Convey("Finish should block context work", t, func() {
		q, _ := CreateNewQueueTestContext()
		So(q.Call("CMD", []string{}).StringResponse(), ShouldContainSubstring, "CMD")
		q.Finish()
		So(q.Call("CMD", []string{}), ShouldEqual, mpqerr.ERR_CONN_CLOSING)
	})
}

type TestResponseWriter struct {
	mutex     sync.Mutex
	responses []apis.IResponse
}

func (rw *TestResponseWriter) WriteResponse(resp apis.IResponse) error {
	rw.mutex.Lock()
	rw.responses = append(rw.responses, resp)
	rw.mutex.Unlock()
	return nil
}

func (rw *TestResponseWriter) GetResponses() []apis.IResponse {
	return rw.responses
}

func NewTestResponseWriter() *TestResponseWriter {
	return &TestResponseWriter{
		responses: make([]apis.IResponse, 0, 1000),
	}
}
*/