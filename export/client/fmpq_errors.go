package client

import "fmt"

type FireMpqError struct {
	Code int64
	Desc string
}

func NewFireMpqError(code int64, desc string) *FireMpqError {
	return &FireMpqError{Code: code, Desc: desc}
}

func (e *FireMpqError) Error() string {
	return fmt.Sprintf("FMPQERR %d:%s", e.Code, e.Desc)
}

func UnexpectedErrorFormat(tokens []string) *FireMpqError {
	return NewFireMpqError(-1, fmt.Sprintf("Unexpected error format: %s", tokens))
}

func WrongDataFormatError(dataType string, value string) *FireMpqError {
	return NewFireMpqError(-2, fmt.Sprintf("Wrong %s format: %s", dataType, value))
}

func UnexpectedResponse(tokens []string) *FireMpqError {
	return NewFireMpqError(-3, fmt.Sprintf("Unexpected response: %s", tokens))
}

func WrongMessageFormatError(msg string) *FireMpqError {
	return NewFireMpqError(-4, msg)
}
