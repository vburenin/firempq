package client

import (
	"strconv"
)

func ParseInt(v string) (int64, error) {
	if v[0] == ':' {
		v = v[1:]
	}

	if v, err := strconv.ParseInt(v, 10, 0); err == nil {
		return v, nil
	}

	return 0, WrongDataFormatError("int type", v)
}

func ParseArraySize(v string) (int64, error) {
	if v[0] == '*' {
		if v, err := strconv.ParseInt(v[1:], 10, 0); err == nil {
			return v, nil
		}
	}

	return 0, WrongDataFormatError("array size", v)
}

func ParseMapSize(v string) (int64, error) {
	if v[0] == '%' {
		if v, err := strconv.ParseInt(v[1:], 10, 0); err == nil {
			return v, nil
		}
	}

	return 0, WrongDataFormatError("map size", v)
}
