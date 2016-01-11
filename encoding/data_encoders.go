package encoding

import (
	"fmt"
	"strconv"
	"strings"

	. "firempq/utils"
)

// EncodeRespString encode string into protocol text format.
func EncodeRespString(data string) string {
	output := []string{"$", strconv.Itoa(len(data)), " ", data}
	return strings.Join(output, "")
}

// EncodeRespInt64 encode int64 into the protocol text format.
func EncodeRespInt64(val int64) string {
	return ":" + strconv.FormatInt(val, 10)
}

// EncodeTo36Base creates a string value of service INT id.
func EncodeTo36Base(exportId uint64) string {
	return strconv.FormatUint(exportId, 36)
}

// EncodeUint64ToString encodes uint64 to the sequence of bytes.
func Sn2Bin(v uint64) string {
	b := make([]byte, 8)
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
	return UnsafeBytesToString(b)
}

// DecodeBytesToUnit64 decodes sequence of bytes to uint64.
func DecodeBytesToUnit64(b []byte) uint64 {
	return (uint64(b[7]) |
		uint64(b[6])<<8 |
		uint64(b[5])<<16 |
		uint64(b[4])<<24 |
		uint64(b[3])<<32 |
		uint64(b[2])<<40 |
		uint64(b[1])<<48 |
		uint64(b[0])<<56)
}

func EncodeUint64(v uint64) string {
	return " :" + strconv.FormatUint(v, 10)
}

func EncodeInt64(v int64) string {
	return " :" + strconv.FormatInt(v, 10)
}

func EncodeBool(v bool) string {
	if v {
		return " ?t"
	} else {
		return " ?f"
	}
}

func EncodeString(v string) string {
	return " $" + strconv.Itoa(len(v)) + " " + v
}

func EncodeMapSize(v int) string {
	return " %" + strconv.Itoa(v)
}

func EncodeArraySize(v int) string {
	return " *" + strconv.Itoa(v)
}

func EncodeError(errorCode int64, errorText string) string {
	return fmt.Sprintf("-ERR %s %s", EncodeRespInt64(errorCode), EncodeRespString(errorText))
}
