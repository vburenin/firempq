package client

import "fmt"

type Error struct {
	Code int64
	Desc string
}

func NewError(code int64, desc string) *Error {
	return &Error{Code: code, Desc: desc}
}

func (e *Error) Error() string {
	return fmt.Sprintf("ERR %d:%s", e.Code, e.Desc)
}

func UnexpectedErrorFormat(tokens []string) *Error {
	return NewError(-1, fmt.Sprintf("unexpected error format: %s", tokens))
}

func WrongDataFormatError(dataType string, value string) *Error {
	return NewError(-2, fmt.Sprintf("wrong %s format: %s", dataType, value))
}

func UnexpectedResponse(tokens []string) *Error {
	return NewError(-3, fmt.Sprintf("unexpected response: %s", tokens))
}

func WrongMessageFormatError(msg string) *Error {
	return NewError(-4, msg)
}
