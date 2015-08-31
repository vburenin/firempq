package util


import (
	"strconv"
	"bufio"
	"unsafe"
)

const (
	STATE_NONE = 0
	STATE_PARSE_TEXT_TOKEN = 1
	STATE_PARSE_BINARY_HEADER = 3
	STATE_PARSE_BINARY_PAYLOAD = 4
	STATE_LOOKING_FOR_LF = 5
	SYMBOL_CR = 0x0D
	SYMBOL_LF = 0x0A
)

const (
	MAX_TOKENS_PER_MSG = 128
	MAX_RECV_BUFFER_SIZE = 4096
	MAX_TEXT_TOKEN_LEN = 256
	MAX_BINARY_TOKEN_LEN = 0x80000
	START_ASCII_RANGE = 0x21
	END_ASCII_RANGE = 0x7E
)


const (
	ERR_TOK_OK = 0
	ERR_TOK_TOO_MANY_TOKENS = 1
	ERR_TOK_TOKEN_TOO_LONG = 2
	ERR_TOK_PARSING_ERROR = 3
	ERR_TOK_IO_ERROR = 4
)

type Tokenizer struct {
	state uint8
	buffer []byte
	bufferCursorPos int
	bufferDataLen int
	reader* bufio.Reader
	binaryDataLen int
	currToken []byte
	tokenCursorPos int
}

func UnsafeBytesToString(b []byte) *string {
	return (*string)(unsafe.Pointer(&b))
}

func initTokenizer(netReader* bufio.Reader) *Tokenizer {
	tok := Tokenizer {
		state: STATE_NONE,
		buffer: make([]byte, MAX_RECV_BUFFER_SIZE),
		bufferCursorPos: 0,
		bufferDataLen: 0,
		binaryDataLen: 0,
		reader: netReader,
		currToken: nil,
		tokenCursorPos: 0,
	}
	return &tok
}

func (tok *Tokenizer) resetTokenizer() {
	tok.state = STATE_NONE
	tok.buffer = nil
	tok.bufferCursorPos = 0
	tok.bufferDataLen = 0
	tok.binaryDataLen = 0
	tok.tokenCursorPos = 0
	tok.currToken = nil
}

func (tok *Tokenizer) checkForSpecialToken() int {
	if tok.currToken == nil {
		return ERR_TOK_OK
	}
	switch tok.currToken[0] {
	case '$':
		// Binary data marker
		str := string(tok.currToken[1:tok.tokenCursorPos])
		if len, err := strconv.Atoi(str); err == nil {
			if len > 0 {
				tok.state = STATE_PARSE_BINARY_PAYLOAD
				tok.binaryDataLen = len
			} else {
				return ERR_TOK_PARSING_ERROR
			}
		} else {
			return ERR_TOK_PARSING_ERROR
		}
	}
	return ERR_TOK_OK
}

func (tok *Tokenizer) acceptToken(result *[]*string) int {
	tok.state = STATE_NONE
	*result = append(*result, UnsafeBytesToString(tok.currToken[:tok.tokenCursorPos]))
	// Check for special tokens to handle their payload
	if tok.state == STATE_PARSE_TEXT_TOKEN {
		switch tok.currToken[0] {
		case '$':
			if len(*result) > MAX_TOKENS_PER_MSG {
				return ERR_TOK_TOO_MANY_TOKENS
			}
			// Binary data marker
			str := string(tok.currToken[1:tok.tokenCursorPos])
			if len, err := strconv.Atoi(str); err == nil {
				if len > 0 {
					tok.state = STATE_PARSE_BINARY_PAYLOAD
					tok.binaryDataLen = len
					tok.currToken = make([]byte, tok.binaryDataLen, tok.binaryDataLen)
				} else {
					return ERR_TOK_PARSING_ERROR
				}
			} else {
				return ERR_TOK_PARSING_ERROR
			}
		default:
			tok.currToken = nil
		}
	} else {
		tok.currToken = nil
	}
	tok.tokenCursorPos = 0
	return ERR_TOK_OK
}

func (tok *Tokenizer) Tokenize() ([]*string, int) {
	result := make([]*string, 0, MAX_TOKENS_PER_MSG)
	tok.tokenCursorPos = 0
	for {
		if tok.bufferCursorPos >= tok.bufferDataLen {
			// Read more data from the network reader
			var readErr error
			tok.bufferDataLen, readErr = tok.reader.Read(tok.buffer)
			if nil == readErr && tok.bufferDataLen > 0 {
				tok.bufferCursorPos = 0
			} else {
				tok.resetTokenizer()
				return nil, ERR_TOK_IO_ERROR
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
				if tok.tokenCursorPos + availableBytes > MAX_BINARY_TOKEN_LEN {
					tok.resetTokenizer()
					return nil, ERR_TOK_TOKEN_TOO_LONG
				}
				copy(tok.currToken[tok.tokenCursorPos:],
					tok.buffer[tok.bufferCursorPos:(tok.bufferCursorPos + availableBytes)])
				tok.tokenCursorPos += availableBytes
				tok.binaryDataLen -= availableBytes
				if tok.binaryDataLen <= 0 {
					// Token complete
					tok.acceptToken(&result)
				}
				tok.bufferCursorPos += availableBytes
				break
			case STATE_LOOKING_FOR_LF:
				tok.resetTokenizer()
				if SYMBOL_LF == val {
					return result, ERR_TOK_OK
				} else {
					return nil, ERR_TOK_PARSING_ERROR
				}
				break
			default:
				if (val >= START_ASCII_RANGE && val <= END_ASCII_RANGE) {
					if nil == tok.currToken {
						// Start parsing new text token
						if len(result) > MAX_TOKENS_PER_MSG {
							tok.resetTokenizer()
							return nil, ERR_TOK_TOO_MANY_TOKENS
						}
						tok.state = STATE_PARSE_TEXT_TOKEN
						tok.currToken = make([]byte, MAX_TEXT_TOKEN_LEN, MAX_TEXT_TOKEN_LEN)
						tok.tokenCursorPos = 0
					}
					if (tok.tokenCursorPos < MAX_TEXT_TOKEN_LEN) {
						tok.currToken[tok.tokenCursorPos] = val
						tok.tokenCursorPos += 1
					} else {
						tok.resetTokenizer()
						return nil, ERR_TOK_TOKEN_TOO_LONG
					}
				} else {
					if (STATE_PARSE_TEXT_TOKEN == tok.state && tok.tokenCursorPos > 0) {
						// Current token completed. Add it to the result array and check for additional payload
						// specific for this token
						if err := tok.acceptToken(&result); err != ERR_TOK_OK {
							tok.resetTokenizer()
							return nil, ERR_TOK_PARSING_ERROR
						}
					}
					if SYMBOL_CR == val {
						tok.state = STATE_LOOKING_FOR_LF
					}
				}
			}
		}
	}
	return result, ERR_TOK_OK
}


