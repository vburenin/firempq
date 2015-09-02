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
	SYMBOL_CR                  = 0x0D
	SYMBOL_LF                  = 0x0A
)

const (
	MAX_TOKENS_PER_MSG   = 128
	MAX_RECV_BUFFER_SIZE = 4096
	MAX_TEXT_TOKEN_LEN   = 8192
	MAX_BINARY_TOKEN_LEN = 0x80000
	START_ASCII_RANGE    = 0x21
	END_ASCII_RANGE      = 0x7E
)

var ERR_TOK_TOO_MANY_TOKENS = errors.New("Too many tokens")
var ERR_TOK_TOKEN_TOO_LONG = errors.New("Token is too long")
var ERR_TOK_PARSING_ERROR = errors.New("Error during token parsing")

type Tokenizer struct {
	state           uint8
	buffer          []byte
	bufferCursorPos int
	bufferDataLen   int
	reader          io.Reader
	binaryDataLen   int
	curToken        []byte
	curTokenLen     int
}

func UnsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func NewTokenizer(reader io.Reader) *Tokenizer {
	tok := Tokenizer{
		state:           STATE_NONE,
		buffer:          make([]byte, MAX_RECV_BUFFER_SIZE),
		bufferCursorPos: 0,
		bufferDataLen:   0,
		binaryDataLen:   0,
		reader:          reader,
		curToken:        nil,
		curTokenLen:     0,
	}
	return &tok
}

func (tok *Tokenizer) appendToken(result []string) ([]string, error) {
	if len(result) >= MAX_TOKENS_PER_MSG {
		return nil, ERR_TOK_TOO_MANY_TOKENS
	}
	// Check for special tokens to handle their payload
	if tok.state == STATE_PARSE_TEXT_TOKEN {
		switch tok.curToken[0] {
		case '$':
			// Binary data marker
			str := UnsafeBytesToString(tok.curToken[1:tok.curTokenLen])
			binLen, err := strconv.Atoi(str)
			if err == nil && (binLen < 1 || binLen > MAX_BINARY_TOKEN_LEN) {
				return result, ERR_TOK_PARSING_ERROR
			}
			tok.state = STATE_PARSE_BINARY_PAYLOAD
			tok.binaryDataLen = binLen
			tok.curToken = make([]byte, tok.binaryDataLen, tok.binaryDataLen)
		default:
			result = append(result, UnsafeBytesToString(tok.curToken[:tok.curTokenLen]))
			tok.curToken = nil
		}
	} else {
		result = append(result, UnsafeBytesToString(tok.curToken[:tok.curTokenLen]))
		tok.curToken = nil
	}
	tok.curTokenLen = 0
	return result, nil
}

func (tok *Tokenizer) resetTokenizer() {
	tok.state = STATE_NONE
	tok.bufferCursorPos = 0
	tok.bufferDataLen = 0
	tok.binaryDataLen = 0
	tok.curTokenLen = 0
	tok.curToken = nil
}

func (tok *Tokenizer) messageProcessed() {
	tok.state = STATE_NONE
	tok.binaryDataLen = 0
	tok.curTokenLen = 0
	tok.curToken = nil
	tok.bufferCursorPos += 1
}

func (tok *Tokenizer) ReadTokens() ([]string, error) {
	result := make([]string, 0, MAX_TOKENS_PER_MSG)
	tok.curTokenLen = 0
	var err error
	for {
		if tok.bufferCursorPos >= tok.bufferDataLen {
			// Read more data from the network reader
			tok.bufferDataLen, err = tok.reader.Read(tok.buffer)
			if nil == err && tok.bufferDataLen > 0 {
				tok.bufferCursorPos = 0
			} else {
				return nil, err
			}
		}
		// Tokenize content of the buffer
		for ; tok.bufferCursorPos < tok.bufferDataLen; tok.bufferCursorPos += 1 {
			val := tok.buffer[tok.bufferCursorPos]

			switch tok.state {
			case STATE_PARSE_BINARY_PAYLOAD:
				availableBytes := tok.bufferDataLen - tok.bufferCursorPos
				if availableBytes > tok.binaryDataLen {
					availableBytes = tok.binaryDataLen
				}
				// Check for max allowed binary token length
				if tok.curTokenLen+availableBytes > MAX_BINARY_TOKEN_LEN {
					return nil, ERR_TOK_TOKEN_TOO_LONG
				}
				copy(tok.curToken[tok.curTokenLen:],
					tok.buffer[tok.bufferCursorPos:(tok.bufferCursorPos+availableBytes)])
				tok.curTokenLen += availableBytes
				tok.binaryDataLen -= availableBytes

				if tok.binaryDataLen <= 0 {
					// Token complete
					result, err = tok.appendToken(result)
					if err != nil {
						return nil, err
					}
				}

				tok.bufferCursorPos += availableBytes
			case STATE_LOOKING_FOR_LF:
				if SYMBOL_LF == val {
					tok.messageProcessed()
					return result, nil
				} else {
					tok.resetTokenizer()
					return nil, ERR_TOK_PARSING_ERROR
				}
			default:
				if val >= START_ASCII_RANGE && val <= END_ASCII_RANGE {
					if nil == tok.curToken {
						// Start parsing new text token
						tok.state = STATE_PARSE_TEXT_TOKEN
						tok.curToken = make([]byte, MAX_TEXT_TOKEN_LEN, MAX_TEXT_TOKEN_LEN)
						tok.curTokenLen = 0
					}
					if tok.curTokenLen < MAX_TEXT_TOKEN_LEN {
						tok.curToken[tok.curTokenLen] = val
						tok.curTokenLen += 1
					} else {
						tok.resetTokenizer()
						return nil, ERR_TOK_TOKEN_TOO_LONG
					}
				} else {
					if STATE_PARSE_TEXT_TOKEN == tok.state && tok.curTokenLen > 0 {
						// Current token completed. Add it to the result array and check for additional payload
						// specific for this token
						result, err = tok.appendToken(result)
						if err != nil {
							tok.resetTokenizer()
							return nil, err
						}
					}
					if SYMBOL_CR == val {
						// Wait for LF symbol following current CR symbol
						tok.state = STATE_LOOKING_FOR_LF
					}
				}
			}
		}
	}
}
