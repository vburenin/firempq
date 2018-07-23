package proto

import "errors"

func UintToHex(i uint64) []byte {
	b := make([]byte, 16)
	d := 15
	for {
		v := i & 0x0f
		if v < 10 {
			b[d] = byte(0x30 | v)
		} else {
			b[d] = byte(55 + v)
		}
		i >>= 4
		if i == 0 || d == 0 {
			break
		}
		d--
	}
	return b[d:]
}

var invalidByte = errors.New("invalid byte")
var tooLong = errors.New("too long")

func HexToUint(s string) (res uint64, err error) {
	if len(s) > 16 {
		return 0, tooLong
	}
	for _, v := range s {
		res <<= 4
		if v >= '0' && v <= '9' {
			res += uint64(v & 0xf)
		} else if v >= 'A' && v <= 'F' {
			res += uint64(v - 55)
		} else if v >= 'a' && v <= 'f' {
			res += uint64(v - 87)
		} else {
			return 0, invalidByte
		}
	}
	return res, nil
}
