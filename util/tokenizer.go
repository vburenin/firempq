package util

import (
	"errors"
	"io"
	"strconv"
	"unsafe"
)

const (
	STATE_INIT                 = 0
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
	buffer []byte
	bufPos int
	bufLen int
	reader io.Reader
}

func UnsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func NewTokenizer(reader io.Reader) *Tokenizer {
	tok := Tokenizer{
		buffer: make([]byte, MAX_RECV_BUFFER_SIZE),
		bufPos: 0,
		bufLen: 0,
		reader: reader,
	}
	return &tok
}

func (tok *Tokenizer) ReadTokens() ([]string, error) {
	result := make([]string, 0, MAX_TOKENS_PER_MSG)
	var err error
	var token []byte
	var binTokenLen int
	var state uint8 = STATE_INIT

	for {
		if tok.bufPos >= tok.bufLen {
			// Read more data from the network reader
			tok.bufPos = 0
			tok.bufLen, err = tok.reader.Read(tok.buffer)
			if nil != err {
				return nil, err
			}
		}
		// Tokenize content of the buffer

		for tok.bufPos < tok.bufLen {
			if state == STATE_PARSE_BINARY_PAYLOAD {
				availableBytes := tok.bufLen - tok.bufPos
				if availableBytes > binTokenLen {
					availableBytes = binTokenLen
				}
				token = append(token, tok.buffer[tok.bufPos:tok.bufPos+availableBytes]...)
				binTokenLen -= availableBytes

				if binTokenLen <= 0 {
					// Binary token complete
					result = append(result, UnsafeBytesToString(token))
					state = STATE_INIT
				}
				tok.bufPos += availableBytes
				continue
			}

			if state == STATE_INIT {
				token = make([]byte, 0, 64)
				state = STATE_PARSE_TEXT_TOKEN
			}

			val := tok.buffer[tok.bufPos]
			tok.bufPos += 1

			if state == STATE_PARSE_TEXT_TOKEN {
				if val >= START_ASCII_RANGE && val <= END_ASCII_RANGE {
					token = append(token, val)
				} else {
					if len(token) > 0 {
						if token[0] == '$' {
							str := UnsafeBytesToString(token[1:])
							binTokenLen, err = strconv.Atoi(str)
							if err == nil && (binTokenLen < 1 || binTokenLen > MAX_BINARY_TOKEN_LEN) {
								return nil, ERR_TOK_PARSING_ERROR
							}
							token = make([]byte, 0, binTokenLen)
							state = STATE_PARSE_BINARY_PAYLOAD
						} else {
							result = append(result, UnsafeBytesToString(token))
							state = STATE_INIT
							if val == SYMBOL_CR {
								return result, nil
							}
						}
					}
				}
			}
		}
	}
}
