//package util_test
//
//import "testing"
//import (
//	"firempq/util"
//	"fmt"
//	"io"
//)
//
//type TestDataReader struct {
//	datas []string
//	pos   int
//}
//
//func (td *TestDataReader) Read(data []byte) (int, error) {
//	if td.pos >= len(td.datas) {
//		return 0, io.EOF
//	}
//	copy(data, td.datas[td.pos])
//	td.pos++
//	return len(td.datas[td.pos-1]), nil
//}
//
//func TestReadTokens(t *testing.T) {
//	var data []string
//	//txt1 := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
//	for i := 20; i > 0; i-- {
//		txt := "0123456789"
//		data = append(data, txt)
//	}
//	data = append(data, "\n fdsf\n")
//	data = append(data, "$9 012345678 11 22\n")
//	data = append(data, "$9 012345611 1 22\n")
//
//	r := &TestDataReader{data, 0}
//	tok := util.NewTokenizer(r)
//	for {
//		val, err := tok.ReadTokens()
//		if err == io.EOF {
//			break
//		}
//		fmt.Println(val, len(val), len(val), err)
//	}
//
//}

package common

import "testing"
import (
	"io"
)

type TestDataReader struct {
	pos int
}

var TXT = "123123 12312312312 12312312312 12312312312 12312312312 asdasdasdas $10 0123456789 \n"

func (td *TestDataReader) Read(data []byte) (int, error) {
	if td.pos > 1000000 {
		return 0, io.EOF
	}

	copy(data, TXT)
	td.pos += 1
	return len(TXT), nil
}

func TestReadTokens(t *testing.T) {
	println(len(TXT))
	r := &TestDataReader{0}
	startTs := Uts()
	tok := NewTokenizer()
	for {
		_, err := tok.ReadTokens(r)
		//fmt.Println(val, err)
		if err == io.EOF {
			break
		}
	}
	println(Uts() - startTs)

}
