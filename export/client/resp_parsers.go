package client

func ExpectOk(t *TokenUtil) error {
	tokens, err := t.ReadTokens()
	if err != nil {
		return err
	}
	if tokens[0] == "+OK" {
		return nil
	}
	if err := ParseError(tokens); err != nil {
		return err
	}
	return UnexpectedResponse(tokens)
}

func ParseMessageId(tokens []string) (string, error) {
	if len(tokens) < 2 {
		return "", UnexpectedErrorFormat(tokens)
	}
	if tokens[0] == "+MSG" {
		return tokens[1], nil

	}

	if err := ParseError(tokens); err != nil {
		return "", err
	}

	return "", UnexpectedErrorFormat(tokens)
}

func ParseError(tokens []string) error {
	if tokens[0] == "-ERR" {
		if len(tokens) < 3 {
			return UnexpectedErrorFormat(tokens)
		}
		if errcode, err := ParseInt(tokens[1]); err != nil {
			return UnexpectedErrorFormat(tokens)
		} else {
			return NewFireMpqError(errcode, tokens[2])
		}
	}
	return nil
}
