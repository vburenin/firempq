package util

import (
	"errors"
	"io"
	"strconv"
	"unsafe"
)

const (
	STATE_NONE                 = 0
	STATE_PARSE_TEXT_TOKEN     = 1
	STATE_PARSE_BINARY_HEADER  = 3
	STATE_PARSE_BINARY_PAYLOAD = 4
	STATE_LOOKING_FOR_LF       = 5
	SYMBOL_CR                  = 0x0A
)

const (
	MAX_TOKENS_PER_MSG   = 128
	MAX_RECV_BUFFER_SIZE = 4096
	MAX_TEXT_TOKEN_LEN   = 256
	MAX_BINARY_TOKEN_LEN = 0x80000
	START_ASCII_RANGE    = 0x21
	END_ASCII_RANGE      = 0x7E
)

var ERR_TOK_TOO_MANY_TOKENS = errors.New("Too many tokens")
var ERR_TOK_TOKEN_TOO_LONG = errors.New("Token is too long")
var ERR_TOK_PARSING_ERROR = errors.New("Error during token parsing")

type Tokenizer struct {
	state       uint8
	buffer      []byte
	bufPos      int
	bufLen      int
	reader      io.Reader
	binDataLeft int
	curToken    []byte
}

func UnsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func NewTokenizer(reader io.Reader) *Tokenizer {
	tok := Tokenizer{
		state:       STATE_NONE,
		buffer:      make([]byte, MAX_RECV_BUFFER_SIZE),
		bufPos:      0,
		bufLen:      0,
		binDataLeft: 0,
		reader:      reader,
		curToken:    nil,
	}
	return &tok
}

func (tok *Tokenizer) appendToken(result []string) ([]string, error) {
	if len(result) >= MAX_TOKENS_PER_MSG {
		return nil, ERR_TOK_TOO_MANY_TOKENS
	}
	// Check for special tokens to handle their payload
	if tok.state == STATE_PARSE_TEXT_TOKEN && tok.curToken[0] == '$' {
		// Binary data marker
		str := UnsafeBytesToString(tok.curToken[1:])
		binLen, err := strconv.Atoi(str)
		if err == nil && (binLen < 1 || binLen > MAX_BINARY_TOKEN_LEN) {
			return result, ERR_TOK_PARSING_ERROR
		}
		tok.binDataLeft = binLen
		tok.curToken = make([]byte, 0, tok.binDataLeft)
		tok.state = STATE_PARSE_BINARY_PAYLOAD
	} else {
		result = append(result, UnsafeBytesToString(tok.curToken))
		tok.curToken = nil
		tok.state = STATE_PARSE_TEXT_TOKEN
	}
	return result, nil
}

func (tok *Tokenizer) resetTokenizer() {
	tok.state = STATE_NONE
	tok.bufPos = 0
	tok.bufLen = 0
	tok.curToken = nil
}

func (tok *Tokenizer) messageProcessed() {
	tok.state = STATE_NONE
	tok.curToken = nil
	tok.bufPos += 1
}

func (tok *Tokenizer) ReadTokens() ([]string, error) {
	result := make([]string, 0, MAX_TOKENS_PER_MSG)
	var err error
	for {
		if tok.bufPos >= tok.bufLen {
			// Read more data from the network reader
			tok.bufPos = 0
			tok.bufLen, err = tok.reader.Read(tok.buffer)
			if nil != err {
				tok.resetTokenizer()
				return nil, err
			}
		}
		// Tokenize content of the buffer
		for tok.bufPos < tok.bufLen {
			if tok.state == STATE_PARSE_BINARY_PAYLOAD {
				availableBytes := tok.bufLen - tok.bufPos
				if availableBytes > tok.binDataLeft {
					availableBytes = tok.binDataLeft
				}
				tok.curToken = append(tok.curToken, tok.buffer[tok.bufPos:tok.bufPos+availableBytes]...)
				tok.binDataLeft -= availableBytes

				if tok.binDataLeft <= 0 {
					// Token complete
					result, err = tok.appendToken(result)
					if err != nil {
						tok.resetTokenizer()
						return nil, err
					}
				}
				tok.bufPos += availableBytes
				continue
			}

			val := tok.buffer[tok.bufPos]
			tok.bufPos += 1

			if val >= START_ASCII_RANGE && val <= END_ASCII_RANGE {
				if tok.curToken == nil {
					// Start parsing new text token
					tok.state = STATE_PARSE_TEXT_TOKEN
					tok.curToken = make([]byte, 0, 64)
				}
				if len(tok.curToken) < MAX_TEXT_TOKEN_LEN {
					tok.curToken = append(tok.curToken, val)
				} else {
					tok.resetTokenizer()
					return nil, ERR_TOK_TOKEN_TOO_LONG
				}
			} else {
				if tok.state == STATE_PARSE_TEXT_TOKEN && len(tok.curToken) > 0 {
					// Current token completed. Add it to the result array and check for additional payload
					// specific for this token
					result, err = tok.appendToken(result)
					if err != nil {
						tok.resetTokenizer()
						return nil, err
					}
				}
				if val == SYMBOL_CR {
					// Wait for LF symbol following current CR symbol
					tok.messageProcessed()
					return result, nil
				}
			}
		}
	}
}
