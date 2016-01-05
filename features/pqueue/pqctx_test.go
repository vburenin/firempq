package pqueue

import (
	"math"
	"strconv"
	"testing"
	"time"

	"firempq/db"
	"firempq/log"
	"firempq/testutils"

	. "firempq/common"
	. "firempq/conf"
	. "firempq/features/pqueue/pqmsg"
	. "firempq/testutils"

	. "github.com/smartystreets/goconvey/convey"
)

func getCtxConfig() *PQConfig {
	return &PQConfig{
		MaxSize:        100001,
		MsgTtl:         100000,
		DeliveryDelay:  1,
		PopLockTimeout: 10000,
		PopCountLimit:  4,
		LastPushTs:     12,
		LastPopTs:      13,
	}
}

func getCtxDesc() *ServiceDescription {
	return &ServiceDescription{
		ExportId:  10,
		SType:     "PQueue",
		Name:      "name",
		CreateTs:  123,
		Disabled:  false,
		ToDelete:  false,
		ServiceId: "1",
	}
}

func i2a(v int64) string { return strconv.FormatInt(v, 10) }

func CreateQueueTestContext() (*PQContext, *TestResponseWriter) {
	rw := NewTestResponseWriter()
	return InitPQueue(getCtxDesc(), getCtxConfig()).NewContext(rw).(*PQContext), rw
}

func CreateNewQueueTestContext() (*PQContext, *TestResponseWriter) {
	log.InitLogging()
	log.SetLevel(1)
	db.SetDatabase(testutils.NewInMemDBService())
	return CreateQueueTestContext()
}

func TestCtxParsePQConfig(t *testing.T) {
	Convey("All config parameters should be parsed correctly", t, func() {
		Convey("Check all parameters are correct", func() {
			params := []string{
				CPRM_MSG_TTL, "100",
				CPRM_MAX_SIZE, "200",
				CPRM_DELIVERY_DELAY, "300",
				CPRM_POP_LIMIT, "400",
				CPRM_LOCK_TIMEOUT, "500",
			}
			cfg, resp := ParsePQConfig(params)
			VerifyOkResponse(resp)
			So(cfg.MsgTtl, ShouldEqual, 100)
			So(cfg.MaxSize, ShouldEqual, 200)
			So(cfg.DeliveryDelay, ShouldEqual, 300)
			So(cfg.PopCountLimit, ShouldEqual, 400)
			So(cfg.PopLockTimeout, ShouldEqual, 500)
		})
		Convey("Message ttl parse error", func() {
			_, err := ParsePQConfig([]string{CPRM_MSG_TTL, "-1"})
			So(err.GetResponse(), ShouldContainSubstring, i2a(CFG_PQ.MaxMessageTtl))
		})
		Convey("Max size parse error", func() {
			_, err := ParsePQConfig([]string{CPRM_MAX_SIZE, "-1"})
			So(err.GetResponse(), ShouldContainSubstring, i2a(math.MaxInt64))
		})
		Convey("Delivery delay parse error", func() {
			_, err := ParsePQConfig([]string{CPRM_DELIVERY_DELAY, "-1"})
			So(err.GetResponse(), ShouldContainSubstring, i2a(CFG_PQ.MaxDeliveryDelay))
		})
		Convey("Pop limit parse error", func() {
			_, err := ParsePQConfig([]string{CPRM_POP_LIMIT, "-1"})
			So(err.GetResponse(), ShouldContainSubstring, i2a(math.MaxInt64))
		})
		Convey("Lock timeout parse error", func() {
			_, err := ParsePQConfig([]string{CPRM_LOCK_TIMEOUT, "-1"})
			So(err.GetResponse(), ShouldContainSubstring, i2a(CFG_PQ.MaxLockTimeout))
		})
	})
}

func TestCtxPopLock(t *testing.T) {
	Convey("Test POPLOCK command", t, func() {
		q, rw := CreateNewQueueTestContext()
		Convey("Lock timeout error should occure", func() {
			resp := q.Call(PQ_CMD_POPLOCK, []string{PRM_LOCK_TIMEOUT, "-1"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(CFG_PQ.MaxLockTimeout))
		})

		Convey("Limit error should occure", func() {
			resp := q.Call(PQ_CMD_POPLOCK, []string{PRM_LIMIT, "-1"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(CFG_PQ.MaxPopBatchSize))
		})

		Convey("Pop wait error should occure", func() {
			resp := q.Call(PQ_CMD_POPLOCK, []string{PRM_POP_WAIT, "-1"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(CFG_PQ.MaxPopWaitTimeout))
		})

		Convey("Async error should occure", func() {
			resp := q.Call(PQ_CMD_POPLOCK, []string{PRM_ASYNC, "--++--"})
			So(resp.GetResponse(), ShouldContainSubstring, "Only [_a-z")
		})

		Convey("Unknown param error should occure", func() {
			resp := q.Call(PQ_CMD_POPLOCK, []string{"PARAM_PAM", "--++--"})
			So(resp.GetResponse(), ShouldContainSubstring, "Unknown")
		})

		Convey("Async pop should return empty list", func() {
			resp := q.Call(PQ_CMD_POPLOCK, []string{PRM_ASYNC, "a1", PRM_POP_WAIT, "1"})
			So(resp.GetResponse(), ShouldEqual, "+A a1")
			time.Sleep(time.Millisecond * 10)
			So(len(rw.GetResponses()), ShouldEqual, 1)
			So(rw.GetResponses()[0].GetResponse(), ShouldEqual, "+ASYNC a1 +MSGS *0")
		})

		Convey("Pop should return empty list", func() {
			p := []string{PRM_POP_WAIT, "1", PRM_LOCK_TIMEOUT, "100", PRM_LIMIT, "10"}
			resp := q.Call(PQ_CMD_POPLOCK, p)
			VerifyItems(resp, 0)
		})

	})
}

func TestCtxPop(t *testing.T) {
	Convey("Pop command should work", t, func() {
		q, rw := CreateNewQueueTestContext()

		Convey("Limit error should occure", func() {
			resp := q.Call(PQ_CMD_POP, []string{PRM_LIMIT, "-1"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(CFG_PQ.MaxPopBatchSize))
		})

		Convey("Pop wait error should occure", func() {
			resp := q.Call(PQ_CMD_POP, []string{PRM_POP_WAIT, "-1"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(CFG_PQ.MaxPopWaitTimeout))
		})

		Convey("Async error should occure", func() {
			resp := q.Call(PQ_CMD_POP, []string{PRM_ASYNC, "--++--"})
			So(resp.GetResponse(), ShouldContainSubstring, "Only [_a-z")
		})

		Convey("Unknown param error should occure", func() {
			resp := q.Call(PQ_CMD_POP, []string{"PARAM_PAM", "--++--"})
			So(resp.GetResponse(), ShouldContainSubstring, "Unknown")
		})

		Convey("Async pop should return empty list", func() {
			resp := q.Call(PQ_CMD_POP, []string{PRM_ASYNC, "a1", PRM_POP_WAIT, "1"})
			So(resp.GetResponse(), ShouldEqual, "+A a1")
			time.Sleep(time.Millisecond * 10)
			So(len(rw.GetResponses()), ShouldEqual, 1)
			So(rw.GetResponses()[0].GetResponse(), ShouldEqual, "+ASYNC a1 +MSGS *0")
		})

		Convey("Pop async run error because POP WAIT is 0", func() {
			p := []string{PRM_POP_WAIT, "0", PRM_LIMIT, "10", PRM_ASYNC, "id1"}
			resp := q.Call(PQ_CMD_POP, p)
			So(resp.GetResponse(), ShouldContainSubstring, "+ASYNC id1 -ERR")
		})

		Convey("Pop should return empty list", func() {
			p := []string{PRM_POP_WAIT, "1", PRM_LIMIT, "10"}
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
			So(resp, ShouldEqual, ERR_MSG_ID_NOT_DEFINED)
		})

		Convey("Wrong message ID format should be detected", func() {
			resp := q.Call(PQ_CMD_MSG_INFO, []string{PRM_ID, "$"})
			So(resp, ShouldEqual, ERR_ID_IS_WRONG)
		})

		Convey("Wrong message ID not found", func() {
			resp := q.Call(PQ_CMD_MSG_INFO, []string{PRM_ID, "1234"})
			So(resp, ShouldEqual, ERR_MSG_NOT_FOUND)
		})
	})
}

func TestCtxDeleteLockedByID(t *testing.T) {
	Convey("Deleting locked messages by id should work", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("No params provided errors should be returned", func() {
			resp := q.Call(PQ_CMD_DELETE_LOCKED_BY_ID, []string{})
			So(resp, ShouldEqual, ERR_MSG_ID_NOT_DEFINED)
		})

		Convey("Wrong message ID format should be detected", func() {
			resp := q.Call(PQ_CMD_DELETE_LOCKED_BY_ID, []string{PRM_ID, "$"})
			So(resp, ShouldEqual, ERR_ID_IS_WRONG)
		})

		Convey("Message is not locked error should be returned", func() {
			q.Call(PQ_CMD_PUSH, []string{PRM_PAYLOAD, "t", PRM_ID, "id1", PRM_DELAY, "0"})
			resp := q.Call(PQ_CMD_DELETE_LOCKED_BY_ID, []string{PRM_ID, "id1"})
			So(resp, ShouldEqual, ERR_MSG_NOT_LOCKED)
			VerifyServiceSize(q.pq, 1)
		})
		Convey("Unknown param failure", func() {
			resp := q.Call(PQ_CMD_DELETE_BY_ID, []string{"TEST_PARAM"})
			So(resp.GetResponse(), ShouldContainSubstring, "TEST_PARAM")
		})
	})
}

func TestCtxDeleteByID(t *testing.T) {
	Convey("Deleting message by id should work well", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("No params provided errors should be returned", func() {
			resp := q.Call(PQ_CMD_DELETE_BY_ID, []string{})
			So(resp, ShouldEqual, ERR_MSG_ID_NOT_DEFINED)
		})

		Convey("Wrong message ID format should be detected", func() {
			resp := q.Call(PQ_CMD_DELETE_BY_ID, []string{PRM_ID, "$"})
			So(resp, ShouldEqual, ERR_ID_IS_WRONG)
		})

		Convey("Message should be deleted", func() {
			q.Call(PQ_CMD_PUSH, []string{PRM_PAYLOAD, "t", PRM_ID, "id1", PRM_DELAY, "0"})
			resp := q.Call(PQ_CMD_DELETE_BY_ID, []string{PRM_ID, "id1"})
			VerifyOkResponse(resp)
		})
		Convey("Unknown param failure", func() {
			resp := q.Call(PQ_CMD_DELETE_BY_ID, []string{"TEST_PARAM"})
			So(resp.GetResponse(), ShouldContainSubstring, "TEST_PARAM")
		})
	})
}

func TestCtxPush(t *testing.T) {
	Convey("Push command should work fine", t, func() {
		q, rw := CreateNewQueueTestContext()

		Convey("Should not accept messages with underscore prefix", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PRM_ID, "_ab", PRM_PAYLOAD, "p"})
			So(resp, ShouldEqual, ERR_USER_ID_IS_WRONG)
		})

		Convey("Priority should be out of range", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PRM_ID, "ab", PRM_PAYLOAD, "p", PRM_PRIORITY, "-1"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(math.MaxInt64))
		})

		Convey("Message ttl should error", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PRM_ID, "ab", PRM_PAYLOAD, "p", PRM_MSG_TTL, "-1"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(CFG_PQ.MaxMessageTtl))
		})

		Convey("Delivery delay must be out of range", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PRM_ID, "ab", PRM_PAYLOAD, "p", PRM_DELAY, "-1"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(CFG_PQ.MaxDeliveryDelay))
		})
		Convey("Push with sync wait. Push should succed. Nothing special will happen.", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PRM_ID, "ab", PRM_PAYLOAD, "p", PRM_DELAY, "1", PRM_SYNC_WAIT})
			VerifyOkResponse(resp)
			VerifyServiceSize(q.pq, 1)
		})
		Convey("Push async with no wait flag. Should fail with error", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PRM_ID, "ab", PRM_PAYLOAD, "p", PRM_DELAY, "1", PRM_ASYNC, "asid"})
			So(resp.GetResponse(), ShouldContainSubstring, "+ASYNC asid -ERR")
		})
		Convey("Push async with wait flag. Should succed with two responses.", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PRM_ID, "ab", PRM_PAYLOAD, "p", PRM_ASYNC, "asid", PRM_SYNC_WAIT})
			So(resp.GetResponse(), ShouldContainSubstring, "+A asid")
			time.Sleep(time.Millisecond * 10)
			So(rw.GetResponses()[0].GetResponse(), ShouldEqual, "+ASYNC asid +OK")
			VerifyServiceSize(q.pq, 1)
		})
		Convey("Push with unknown param.", func() {
			resp := q.Call(PQ_CMD_PUSH, []string{PRM_ID, "ab", PRM_PAYLOAD, "p", "TEST_PARAM"})
			So(resp.GetResponse(), ShouldContainSubstring, "TEST_PARAM")
		})
	})
}

func TestCtxUpdateLock(t *testing.T) {
	Convey("Update lock should work fine", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("Should fail with unknown param", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_ID, []string{PRM_ID, "ab", "TEST_PARAM"})
			So(resp.GetResponse(), ShouldContainSubstring, "TEST_PARAM")
		})
		Convey("Failure with incorrect message id", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_ID, []string{PRM_ID, "$ab", PRM_LOCK_TIMEOUT, "10000"})
			So(resp, ShouldEqual, ERR_ID_IS_WRONG)
		})

		Convey("Failure with empty message id", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_ID, []string{PRM_LOCK_TIMEOUT, "1"})
			So(resp, ShouldEqual, ERR_MSG_ID_NOT_DEFINED)
		})
		Convey("Failure with no timeout defined", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_ID, []string{PRM_ID, "1234"})
			So(resp, ShouldEqual, ERR_MSG_TIMEOUT_NOT_DEFINED)
		})

		Convey("Failure with no message", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_ID, []string{PRM_ID, "1234", PRM_LOCK_TIMEOUT, "100"})
			So(resp, ShouldEqual, ERR_MSG_NOT_FOUND)
		})

		Convey("Failure with to wrong timeout", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_ID, []string{PRM_ID, "1234", PRM_LOCK_TIMEOUT, "-1"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(CFG_PQ.MaxLockTimeout))
		})
	})
}

func TestCtxUnlockMessageByID(t *testing.T) {
	Convey("Unlocking message should work as expected", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("No params provided errors should be returned", func() {
			resp := q.Call(PQ_CMD_UNLOCK_BY_ID, []string{})
			So(resp, ShouldEqual, ERR_MSG_ID_NOT_DEFINED)
		})

		Convey("Wrong message ID format should be detected", func() {
			resp := q.Call(PQ_CMD_UNLOCK_BY_ID, []string{PRM_ID, "$"})
			So(resp, ShouldEqual, ERR_ID_IS_WRONG)
		})

		Convey("Unknown param failure", func() {
			resp := q.Call(PQ_CMD_UNLOCK_BY_ID, []string{"TEST_PARAM"})
			So(resp.GetResponse(), ShouldContainSubstring, "TEST_PARAM")
		})

		Convey("Message not found error should happend", func() {
			resp := q.Call(PQ_CMD_UNLOCK_BY_ID, []string{PRM_ID, "id1"})
			So(resp, ShouldEqual, ERR_MSG_NOT_FOUND)
		})
	})
}

func TestCtxGetCurrentStatus(t *testing.T) {
	Convey("Get current status error should be returned", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("No params should be provided error should be returned", func() {
			resp := q.Call(PQ_CMD_STATUS, []string{"PRM"})
			So(resp, ShouldEqual, ERR_CMD_WITH_NO_PARAMS)
		})
		Convey("Should return service status", func() {
			_, ok := q.Call(PQ_CMD_STATUS, []string{}).(*DictResponse)
			So(ok, ShouldBeTrue)
		})
	})
}

func TestCtxCheckTimeouts(t *testing.T) {
	Convey("Unlocking message should work as expected", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("TS param needed error should be returned", func() {
			resp := q.Call(PQ_CMD_CHECK_TIMEOUTS, []string{})
			So(resp, ShouldEqual, ERR_TS_PARAMETER_NEEDED)
		})
		Convey("Should return unknown param error", func() {
			resp := q.Call(PQ_CMD_CHECK_TIMEOUTS, []string{"TEST_PARAM"})
			So(resp.GetResponse(), ShouldContainSubstring, "TEST_PARAM")
		})
		Convey("Should return wrong TS error", func() {
			resp := q.Call(PQ_CMD_CHECK_TIMEOUTS, []string{PRM_TIMESTAMP, "-1"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(math.MaxInt64))
		})
		Convey("Should work", func() {
			resp := q.Call(PQ_CMD_CHECK_TIMEOUTS, []string{PRM_TIMESTAMP, "1000"})
			So(resp.GetResponse(), ShouldEqual, "+DATA :0")
		})
	})
}

func TestCtxSetParamValue(t *testing.T) {
	Convey("Set param should work well", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("At least one parameter should be provided error", func() {
			resp := q.Call(PQ_CMD_SET_CFG, []string{})
			So(resp, ShouldEqual, ERR_CMD_PARAM_NOT_PROVIDED)
		})
		Convey("Message TTL error", func() {
			resp := q.Call(PQ_CMD_SET_CFG, []string{CPRM_MSG_TTL, "0"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(CFG_PQ.MaxMessageTtl))
		})
		Convey("Queue Max Size", func() {
			resp := q.Call(PQ_CMD_SET_CFG, []string{CPRM_MAX_SIZE, "-1"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(math.MaxInt64))
		})
		Convey("Message delivery delay error", func() {
			resp := q.Call(PQ_CMD_SET_CFG, []string{CPRM_DELIVERY_DELAY, "-1"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(CFG_PQ.MaxDeliveryDelay))
		})
		Convey("Pop limit out of range error", func() {
			resp := q.Call(PQ_CMD_SET_CFG, []string{CPRM_POP_LIMIT, "-1"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(math.MaxInt64))
		})

		Convey("Lock timeout error", func() {
			resp := q.Call(PQ_CMD_SET_CFG, []string{CPRM_LOCK_TIMEOUT, "-1"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(CFG_PQ.MaxLockTimeout))
		})

		Convey("Should return unknown param error", func() {
			resp := q.Call(PQ_CMD_SET_CFG, []string{"TEST_PARAM"})
			So(resp.GetResponse(), ShouldContainSubstring, "TEST_PARAM")
		})
		Convey("All parameters should be set", func() {
			params := []string{
				CPRM_DELIVERY_DELAY, "100",
				CPRM_MSG_TTL, "10000",
				CPRM_MAX_SIZE, "100000",
			}
			VerifyOkResponse(q.Call(PQ_CMD_SET_CFG, params))
		})
	})
}

func TestCtxDeleteByReceipt(t *testing.T) {
	Convey("All call scenarios should return expected response", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("Should return expired error", func() {
			resp := q.Call(PQ_CMD_DELETE_BY_RCPT, []string{PRM_RECEIPT, "1-2"})
			So(resp, ShouldEqual, ERR_RECEIPT_EXPIRED)
		})
		Convey("Should return invalid receipt error", func() {
			resp := q.Call(PQ_CMD_DELETE_BY_RCPT, []string{PRM_RECEIPT, "!@#$%%"})
			So(resp, ShouldEqual, ERR_INVALID_RECEIPT)
		})
		Convey("Should return no receipt provided error", func() {
			resp := q.Call(PQ_CMD_DELETE_BY_RCPT, []string{})
			So(resp, ShouldEqual, ERR_NO_RECEIPT)
		})
		Convey("Should return unknown parameter error", func() {
			resp := q.Call(PQ_CMD_DELETE_BY_RCPT, []string{"UNKNOWN"})
			So(resp.GetResponse(), ShouldContainSubstring, "UNKNOWN")
		})
	})
}

func TestCtxUnlockByReceipt(t *testing.T) {
	Convey("All call scenarios should return expected response", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("Should return expired error", func() {
			resp := q.Call(PQ_CMD_UNLOCK_BY_RCPT, []string{PRM_RECEIPT, "1-2"})
			So(resp, ShouldEqual, ERR_RECEIPT_EXPIRED)
		})
		Convey("Should return invalid receipt error", func() {
			resp := q.Call(PQ_CMD_UNLOCK_BY_RCPT, []string{PRM_RECEIPT, "!@#$%%"})
			So(resp, ShouldEqual, ERR_INVALID_RECEIPT)
		})
		Convey("Should return no receipt provided error", func() {
			resp := q.Call(PQ_CMD_UNLOCK_BY_RCPT, []string{})
			So(resp, ShouldEqual, ERR_NO_RECEIPT)
		})
		Convey("Should return unknown parameter error", func() {
			resp := q.Call(PQ_CMD_UNLOCK_BY_RCPT, []string{"UNKNOWN"})
			So(resp.GetResponse(), ShouldContainSubstring, "UNKNOWN")
		})
	})
}

func TestCtxUpdateLockByReceipt(t *testing.T) {
	Convey("All call scenarios should return expected response", t, func() {
		q, _ := CreateNewQueueTestContext()
		Convey("Should return no timeout parameter error", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_RCPT, []string{PRM_RECEIPT, "1-2"})
			So(resp, ShouldEqual, ERR_MSG_TIMEOUT_NOT_DEFINED)
		})
		Convey("Should return invalid timeout error", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_RCPT, []string{PRM_RECEIPT, "1-2", PRM_LOCK_TIMEOUT, "-1"})
			So(resp.GetResponse(), ShouldContainSubstring, i2a(CFG_PQ.MaxLockTimeout))
		})
		Convey("Should return unknown parameter error", func() {
			resp := q.Call(PQ_CMD_UPD_LOCK_BY_RCPT, []string{"UNKNOWN"})
			So(resp.GetResponse(), ShouldContainSubstring, "UNKNOWN")
		})
	})
}

func TestCtxFinish(t *testing.T) {
	Convey("Finish should block context work", t, func() {
		q, _ := CreateNewQueueTestContext()
		So(q.Call("CMD", []string{}).GetResponse(), ShouldContainSubstring, "CMD")
		q.Finish()
		So(q.Call("CMD", []string{}), ShouldEqual, ERR_CONN_CLOSING)
	})
}
