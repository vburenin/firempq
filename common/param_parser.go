package common

import (
	"fmt"
	"strconv"
)

func makeIntDesc(valName string, minValue, maxValue int64) string {
	return fmt.Sprintf("Parameter %s must be followed by an integer value in the range of [%d, %d]",
		valName, minValue, maxValue)
}

func ParseInt64Param(params []string, minValue, maxValue int64) ([]string, int64, *ErrorResponse) {
	valName := params[0]
	if len(params) < 2 {
		return nil, 0, InvalidRequest(makeIntDesc(valName, minValue, maxValue))
	}
	val, err := strconv.ParseInt(params[1], 10, 0)

	if err == nil && val >= minValue && val <= maxValue {
		return params[2:], val, nil
	}

	return nil, 0, InvalidRequest(makeIntDesc(valName, minValue, maxValue))
}

func makeStrDesc(valName string, minLen, maxLen int64) string {
	return fmt.Sprintf("Parameter %s must be followed by a string value with the min length %d and max length %d",
		valName, minLen, maxLen)
}

func ParseStringParam(params []string, minLen, maxLen int64) ([]string, string, *ErrorResponse) {
	valName := params[0]
	if len(params) < 2 {
		return nil, "", InvalidRequest(makeStrDesc(valName, minLen, maxLen))
	}
	paramLen := int64(len(params[1]))
	if paramLen >= minLen && paramLen <= maxLen {
		return params[2:], params[1], nil
	}
	return nil, "", InvalidRequest(makeStrDesc(valName, minLen, maxLen))
}

// ParseUserItemId parses user provided item id that can not start with '_'.
func ParseUserItemId(params []string) ([]string, string, *ErrorResponse) {
	if len(params) >= 2 {
		if ValidateUserItemId(params[1]) {
			return params[2:], params[1], nil
		} else {
			return nil, "", ERR_USER_ID_IS_WRONG
		}
	}
	return nil, "", ERR_USER_ID_IS_WRONG
}

// ParseItemId parses item id that can use all characters.
func ParseItemId(params []string) ([]string, string, *ErrorResponse) {
	if len(params) >= 2 {
		if ValidateItemId(params[1]) {
			return params[2:], params[1], nil
		} else {
			return nil, "", ERR_ID_IS_WRONG
		}
	}
	return nil, "", ERR_ID_IS_WRONG
}

func ParseServiceType(params []string) ([]string, string, *ErrorResponse) {
	valName := params[0]
	if len(params) >= 2 {
		svcType := params[1]
		if svcType != "pqueue" && svcType != "pq" {
			return nil, "", InvalidRequest("Unknown service type: " + svcType)
		}
		return params[2:], svcType, nil
	}
	return nil, "", InvalidRequest(valName + " must be followed by service type")
}

func Parse36BaseUIntValue(v string) (uint64, error) {
	return strconv.ParseUint(v, 36, 0)
}

func Parse36BaseIntValue(v string) (int64, error) {
	return strconv.ParseInt(v, 36, 0)
}

func ParseReceiptParam(params []string) ([]string, string, *ErrorResponse) {
	return ParseStringParam(params, 3, 256)
}
