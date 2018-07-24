package proto

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
	maxTokensPerMsg   = 256
	maxRecvBufferSize = 4096
	maxTextTokenLen   = 256
	maxBinaryTokenLen = 600 * 1024
	startAsciiRange   = 0x21
	endAsciiRange     = 0x7E
)

type Tokenizer struct {
	buffer   []byte
	bufPos   int
	bufLen   int
	tokenBuf []byte
}

func NewTokenizer() *Tokenizer {
	tok := Tokenizer{
		buffer:   make([]byte, maxRecvBufferSize),
		bufPos:   0,
		bufLen:   0,
		tokenBuf: make([]byte, 128),
	}
	return &tok
}

func (tok *Tokenizer) ReadTokens(reader io.Reader) ([]string, error) {
	var err error
	var binTokenLen int
	var state = stateParseTextToken

	tok.tokenBuf = tok.tokenBuf[0:0]
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
			tok.tokenBuf = append(tok.tokenBuf, tok.buffer[tok.bufPos:tok.bufPos+availableBytes]...)
			binTokenLen -= availableBytes

			tok.bufPos += availableBytes

			if binTokenLen <= 0 {
				// Binary token complete
				state = stateParseTextToken
				result = append(result, string(tok.tokenBuf))
				if len(result) > maxTokensPerMsg {
					return nil, mpqerr.ErrTokTooManyTokens
				}
				tok.tokenBuf = tok.tokenBuf[0:0]
			}
			continue
		}

		val := tok.buffer[tok.bufPos]
		tok.bufPos += 1

		if val >= startAsciiRange && val <= endAsciiRange {
			tok.tokenBuf = append(tok.tokenBuf, val)
		} else if len(tok.tokenBuf) > 0 {
			if tok.tokenBuf[0] == '$' {
				binTokenLen, err = strconv.Atoi(string(tok.tokenBuf[1:]))
				if err == nil && (binTokenLen < 1 || binTokenLen > maxBinaryTokenLen) {
					return nil, mpqerr.ErrTokInvalid
				}
				state = stateParseBinaryPayload
				tok.tokenBuf = make([]byte, 0, binTokenLen)
			} else {
				result = append(result, string(tok.tokenBuf))
				if len(result) > maxTokensPerMsg {
					return nil, mpqerr.ErrTokTooManyTokens
				}
				if val == symbolCr {
					return result, nil
				}
				tok.tokenBuf = tok.tokenBuf[0:0]
			}
		} else {
			if val == symbolCr {
				return result, nil
			}
		}
		if len(tok.tokenBuf) > maxTextTokenLen {
			return nil, mpqerr.ErrTokTooLong
		}
	}
}
