package parsers

import (
	"io"
	"testing"

	. "firempq/errors"

	. "github.com/smartystreets/goconvey/convey"
)

type TestDataReader struct {
	pos      int
	testdata string
}

func NewTestDataReader(data string) *TestDataReader {
	return &TestDataReader{0, data}
}

func (td *TestDataReader) Read(data []byte) (int, error) {
	if len(td.testdata) == 0 {
		return 0, io.EOF
	}
	l := 0
	copy(data, td.testdata)
	if len(td.testdata) < len(data) {
		l = len(td.testdata)
	} else {
		l = len(data)
	}
	td.testdata = td.testdata[l:]
	return l, nil
}

func TestReadTokens(t *testing.T) {
	Convey("All tokens should be read correctly", t, func() {
		tok := NewTokenizer()
		Convey("Read two lines of three simple tokens in each line", func() {
			r := NewTestDataReader("item1 item2 item3\ni2 i3 i4\n")
			tokens, _ := tok.ReadTokens(r)
			So(tokens, ShouldResemble, []string{"item1", "item2", "item3"})
			tokens, _ = tok.ReadTokens(r)
			So(tokens, ShouldResemble, []string{"i2", "i3", "i4"})
		})
		Convey("Two token in line should be read correctly", func() {
			r := NewTestDataReader("data1 $5 dd tt\n")
			tokens, _ := tok.ReadTokens(r)
			So(tokens, ShouldResemble, []string{"data1", "dd tt"})
		})
		Convey("Three complex tokens in line", func() {
			r := NewTestDataReader("$5 data1 $6 test  $3 a z\n")
			tokens, _ := tok.ReadTokens(r)
			So(tokens, ShouldResemble, []string{"data1", "test  ", "a z"})
		})
	})
}

func TestIncompleteReads(t *testing.T) {
	Convey("Incomplete content with no closed line", t, func() {
		tok := NewTokenizer()
		Convey("Two tokens no end", func() {
			_, err := tok.ReadTokens(NewTestDataReader("item1 item2"))
			So(err.Error(), ShouldEqual, "EOF")
		})
		Convey("One complex token with no end", func() {
			_, err := tok.ReadTokens(NewTestDataReader("$20 asdkasd"))
			So(err.Error(), ShouldEqual, "EOF")
		})
	})
}

func TestTooMuchData(t *testing.T) {
	Convey("Too much data is not ok", t, func() {
		tok := NewTokenizer()
		Convey("To many tokens", func() {
			d := "a b c d e f g h i j k l m n o p q r s t u v w x w z"
			d += d
			d += "\n"
			_, err := tok.ReadTokens(NewTestDataReader(d))
			So(err, ShouldEqual, ERR_TOK_TOO_MANY_TOKENS)
		})
		Convey("Token is too large", func() {
			data := make([]byte, 129*1024)
			for i := 0; i < 129*1024; i++ {
				data[i] = 'a'
			}
			_, err := tok.ReadTokens(NewTestDataReader(string(data)))
			So(err, ShouldEqual, ERR_TOK_TOKEN_TOO_LONG)
		})
	})
}

func TestSingleChar(t *testing.T) {
	Convey("Too much data is not ok", t, func() {
		d := "CTX $1 c\n"
		tok := NewTokenizer()
		v, _ := tok.ReadTokens(NewTestDataReader(d))
		So(v, ShouldResemble, []string{"CTX", "c"})
	})
}
