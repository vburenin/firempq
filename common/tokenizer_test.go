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
