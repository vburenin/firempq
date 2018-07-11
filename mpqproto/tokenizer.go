package mpqproto

import (
	"io"
	"strconv"

	"github.com/vburenin/firempq/mpqerr"
)

const (
	stateParseTextToken     = 1
	stateParseBinaryPayload = 2
	symbolCr                = 0x0A
)

const (
	maxTokensPerMsg    = 100
	maxRecvBufferSize  = 4096
	maxTextTokenLen    = 256
	maxBinaryTokenLen  = 128 * 1024 * 1024
	startAsciiRange    = 0x21
	endAsciiRange      = 0x7E
	initTokenBufferLen = 48
)

type Tokenizer struct {
	buffer []byte
	bufPos int
	bufLen int
}

func NewTokenizer() *Tokenizer {
	tok := Tokenizer{
		buffer: make([]byte, maxRecvBufferSize),
		bufPos: 0,
		bufLen: 0,
	}
	return &tok
}

func (tok *Tokenizer) ReadTokens(reader io.Reader) ([]string, error) {
	var err error
	var token = make([]byte, 0, initTokenBufferLen)
	var binTokenLen int
	var state = stateParseTextToken

	result := make([]string, 0, maxTokensPerMsg)

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
		if state == stateParseBinaryPayload {
			availableBytes := tok.bufLen - tok.bufPos
			if availableBytes > binTokenLen {
				availableBytes = binTokenLen
			}
			token = append(token, tok.buffer[tok.bufPos:tok.bufPos+availableBytes]...)
			binTokenLen -= availableBytes

			tok.bufPos += availableBytes

			if binTokenLen <= 0 {
				// Binary token complete
				state = stateParseTextToken
				result = append(result, string(token))
				if len(result) > maxTokensPerMsg {
					return nil, mpqerr.ErrTokTooManyTokens
				}
				token = token[0:0]
			}
			continue
		}

		val := tok.buffer[tok.bufPos]
		tok.bufPos += 1

		if val >= startAsciiRange && val <= endAsciiRange {
			token = append(token, val)
		} else if len(token) > 0 {
			if token[0] == '$' {
				binTokenLen, err = strconv.Atoi(string(token[1:]))
				if err == nil && (binTokenLen < 1 || binTokenLen > maxBinaryTokenLen) {
					return nil, mpqerr.ErrTokInvalid
				}
				state = stateParseBinaryPayload
				token = make([]byte, 0, binTokenLen)
			} else {
				result = append(result, string(token))
				if len(result) > maxTokensPerMsg {
					return nil, mpqerr.ErrTokTooManyTokens
				}
				if val == symbolCr {
					return result, nil
				}
				token = token[0:0]
			}
		} else {
			if val == symbolCr {
				return result, nil
			}
		}
		if len(token) > maxTextTokenLen {
			return nil, mpqerr.ErrTokTooLong
		}
	}
}
