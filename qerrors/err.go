package qerrors


type ServerError struct {
    ErrorText string
    ErrorCode uint32
}


func NewError(errorText string, errorCode uint32) *ServerError{
    return &ServerError{ErrorText: errorText, ErrorCode: errorCode}
}

func (e *ServerError) Error() string {
    return e.ErrorText
}


var ERR_LARGE_REQ = &ServerError{"Too large request!", 400}
var DISCONNECT = &ServerError{"Disconnect requested", 200}


var ERR_NO_QUEUE = &ServerError{"Queue does't exist!", 400}
var ERR_QUEUE_ALREADY_EXISTS = &ServerError{"Queue exists already", 400}
var ERR_ITEM_ALREADY_EXISTS = &ServerError{"Message exists already", 400}
var ERR_UNEXPECTED_PRIORITY = &ServerError{"Incrorrect priority", 400}
var ERR_MSG_NOT_LOCKED = &ServerError{"Message is not locked", 400}
var ERR_MSG_NOT_EXIST = &ServerError{"Message doesn't exist", 400}
var ERR_MSG_IS_LOCKED = &ServerError{"Message is locked", 400}
var ERR_MSG_POP_ATTEMPTS_EXCEEDED = &ServerError{"Message is locked", 400}

