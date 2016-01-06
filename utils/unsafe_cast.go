package utils

import (
	"unsafe"
)

func UnsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func UnsafeStringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}
