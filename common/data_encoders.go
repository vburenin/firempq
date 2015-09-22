package common

import (
	"strconv"
	"strings"
)

func EncodeRespString(data string) string {
	output := []string{"$", strconv.Itoa(len(data)), " ", data}
	return strings.Join(output, "")
}

func EncodeRespInt64(val int64) string {
	return ":" + strconv.FormatInt(val, 10)
}
