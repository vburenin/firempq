package response_encoder

import "strconv"

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
