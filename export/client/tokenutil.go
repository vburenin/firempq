package client

import (
	"bufio"
	"net"
	"strconv"

	"github.com/vburenin/firempq/export/encoding"
	"github.com/vburenin/firempq/export/proto"
)

type TokenUtil struct {
	tokReader *proto.Tokenizer
	conn      net.Conn
	buf       *bufio.Writer
}

func (t *TokenUtil) ReadTokens() ([]string, error) {
	return t.tokReader.ReadTokens(t.conn)
}

func (t *TokenUtil) Complete() error {
	t.buf.WriteByte('\n')
	return t.buf.Flush()
}

func (t *TokenUtil) Close() error {
	if err := t.buf.Flush(); err != nil {
		return err
	}
	return t.conn.Close()
}

func (t *TokenUtil) SendInt(i int64) error {
	_, err := t.buf.WriteString(strconv.FormatInt(i, 10))
	return err
}

func (t *TokenUtil) SendString(s string) error {
	return encoding.WriteString(t.buf, s)
}

func (t *TokenUtil) SendTokenWithStringParam(token, s string) error {
	t.buf.WriteString(token)
	t.buf.WriteByte(' ')
	return t.SendString(s)
}

func (t *TokenUtil) SendCompleteTokenWithString(token, s string) error {
	t.buf.WriteString(token)
	t.buf.WriteByte(' ')
	t.SendString(s)
	return t.Complete()
}

func (t *TokenUtil) SendToken(token string) error {
	_, err := t.buf.WriteString(token)
	return err
}

func (t *TokenUtil) SendTokenWithSpace(token string) error {
	t.buf.WriteString(token)
	return t.buf.WriteByte(' ')
}

func (t *TokenUtil) SendSpace() error {
	return t.buf.WriteByte(' ')
}
