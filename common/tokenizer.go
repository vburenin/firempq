package common

import (
	"errors"
	"io"
	"strconv"
)

const (
	STATE_PARSE_TEXT_TOKEN     = 1
	STATE_PARSE_BINARY_PAYLOAD = 2
	SYMBOL_CR                  = 0x0A
)

const (
	MAX_TOKENS_PER_MSG    = 32
	MAX_RECV_BUFFER_SIZE  = 4096
	MAX_TEXT_TOKEN_LEN    = 256
	MAX_BINARY_TOKEN_LEN  = 128 * 1024 * 1024
	START_ASCII_RANGE     = 0x21
	END_ASCII_RANGE       = 0x7E
	INIT_TOKEN_BUFFER_LEN = 48
)

var ERR_TOK_TOO_MANY_TOKENS = errors.New("Too many tokens")
var ERR_TOK_TOKEN_TOO_LONG = errors.New("Token is too long")
var ERR_TOK_PARSING_ERROR = errors.New("Error during token parsing")

type Tokenizer struct {
	buffer []byte
	bufPos int
	bufLen int
}

func NewTokenizer() *Tokenizer {
	tok := Tokenizer{
		buffer: make([]byte, MAX_RECV_BUFFER_SIZE),
		bufPos: 0,
		bufLen: 0,
	}
	return &tok
}

func (tok *Tokenizer) ReadTokens(reader io.Reader) ([]string, error) {
	var err error
	var token []byte = make([]byte, 0, INIT_TOKEN_BUFFER_LEN)
	var binTokenLen int
	var state int = STATE_PARSE_TEXT_TOKEN

	result := make([]string, 0, MAX_TOKENS_PER_MSG)

	for {
		if tok.bufPos >= tok.bufLen {
			// Read more data from the network reader
			tok.bufPos = 0
			tok.bufLen, err = reader.Read(tok.buffer)
			if nil != err {
				return nil, err
			}
			if tok.bufLen == 0 {
				continue
			}
		}

		// Tokenize content of the buffer
		if state == STATE_PARSE_BINARY_PAYLOAD {
			availableBytes := tok.bufLen - tok.bufPos
			if availableBytes > binTokenLen {
				availableBytes = binTokenLen
			}
			token = append(token, tok.buffer[tok.bufPos:tok.bufPos+availableBytes]...)
			binTokenLen -= availableBytes

			tok.bufPos += availableBytes

			if binTokenLen <= 0 {
				// Binary token complete
				state = STATE_PARSE_TEXT_TOKEN
				result = append(result, UnsafeBytesToString(token))
				if len(result) > MAX_TOKENS_PER_MSG {
					return nil, ERR_TOK_TOO_MANY_TOKENS
				}
				token = make([]byte, 0, INIT_TOKEN_BUFFER_LEN)
			}
			continue
		}

		val := tok.buffer[tok.bufPos]
		tok.bufPos += 1

		if val >= START_ASCII_RANGE && val <= END_ASCII_RANGE {
			token = append(token, val)
		} else if len(token) > 0 {
			if token[0] == '$' {
				binTokenLen, err = strconv.Atoi(UnsafeBytesToString(token[1:]))
				if err == nil && (binTokenLen < 1 || binTokenLen > MAX_BINARY_TOKEN_LEN) {
					return nil, ERR_TOK_PARSING_ERROR
				}
				state = STATE_PARSE_BINARY_PAYLOAD
				token = make([]byte, 0, binTokenLen)
			} else {
				result = append(result, UnsafeBytesToString(token))
				if len(result) > MAX_TOKENS_PER_MSG {
					return nil, ERR_TOK_TOO_MANY_TOKENS
				}
				if val == SYMBOL_CR {
					return result, nil
				}
				token = make([]byte, 0, INIT_TOKEN_BUFFER_LEN)
			}
		} else {
			if val == SYMBOL_CR {
				return result, nil
			}
		}
		if len(token) > MAX_TEXT_TOKEN_LEN {
			return nil, ERR_TOK_TOKEN_TOO_LONG
		}
	}
}
