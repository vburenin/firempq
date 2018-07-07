package mpqproto

import (
	"fmt"
	"strconv"

	"github.com/vburenin/firempq/mpqerr"
)

func makeIntDesc(valName string, minValue, maxValue int64) string {
	return fmt.Sprintf("Parameter %s must be followed by an integer value in the range of [%d, %d]",
		valName, minValue, maxValue)
}

func ParseInt64Param(params []string, minValue, maxValue int64) ([]string, int64, *mpqerr.ErrorResponse) {
	valName := params[0]
	if len(params) < 2 {
		return nil, 0, mpqerr.InvalidRequest(makeIntDesc(valName, minValue, maxValue))
	}
	val, err := strconv.ParseInt(params[1], 10, 0)

	if err == nil && val >= minValue && val <= maxValue {
		return params[2:], val, nil
	}

	return nil, 0, mpqerr.InvalidRequest(makeIntDesc(valName, minValue, maxValue))
}

func makeStrDesc(valName string, minLen, maxLen int64) string {
	return fmt.Sprintf("Parameter %s must be followed by a string value with the min length %d and max length %d",
		valName, minLen, maxLen)
}

func ParseStringParam(params []string, minLen, maxLen int64) ([]string, string, *mpqerr.ErrorResponse) {
	valName := params[0]
	if len(params) < 2 {
		return nil, "", mpqerr.InvalidRequest(makeStrDesc(valName, minLen, maxLen))
	}
	paramLen := int64(len(params[1]))
	if paramLen >= minLen && paramLen <= maxLen {
		return params[2:], params[1], nil
	}
	return nil, "", mpqerr.InvalidRequest(makeStrDesc(valName, minLen, maxLen))
}

// ParseUserItemId parses user provided item id that can not start with '_'.
func ParseUserItemId(params []string) ([]string, string, *mpqerr.ErrorResponse) {
	if len(params) >= 2 {
		if ValidateUserItemId(params[1]) {
			return params[2:], params[1], nil
		} else {
			return nil, "", mpqerr.ErrInvalidUserID
		}
	}
	return nil, "", mpqerr.ErrInvalidUserID
}

// ParseItemId parses item id that can use all characters.
func ParseItemId(params []string) ([]string, string, *mpqerr.ErrorResponse) {
	if len(params) >= 2 {
		if ValidateItemId(params[1]) {
			return params[2:], params[1], nil
		} else {
			return nil, "", mpqerr.ErrInvalidID
		}
	}
	return nil, "", mpqerr.ErrInvalidID
}

func ParseServiceType(params []string) ([]string, string, *mpqerr.ErrorResponse) {
	valName := params[0]
	if len(params) >= 2 {
		svcType := params[1]
		if svcType != "pqueue" && svcType != "pq" {
			return nil, "", mpqerr.InvalidRequest("Unknown service type: " + svcType)
		}
		return params[2:], svcType, nil
	}
	return nil, "", mpqerr.InvalidRequest(valName + " must be followed by service type")
}

func Parse36BaseUIntValue(v string) (uint64, error) {
	return strconv.ParseUint(v, 36, 0)
}

func Parse36BaseIntValue(v string) (int64, error) {
	return strconv.ParseInt(v, 36, 0)
}

func ParseReceiptParam(params []string) ([]string, string, *mpqerr.ErrorResponse) {
	return ParseStringParam(params, 3, 256)
}
