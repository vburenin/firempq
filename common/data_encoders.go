package common

import (
	"strconv"
	"strings"
)

func EncodeRespStringTo(data []string, str string) []string {
	return []string{"$", strconv.Itoa(len(str)), " ", str}
}

func EncodeRespInt64To(data []string, val int64) []string {
	return []string{":", strconv.FormatInt(val, 10)}
}

func EncodeRespString(data string) string {
	output := []string{"$", strconv.Itoa(len(data)), " ", data}
	return strings.Join(output, "")
}

func EncodeRespInt64(val int64) string {
	return ":" + strconv.FormatInt(val, 10)
}

func EncodeUint64ToString(v uint64) string {
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
