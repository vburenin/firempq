package common

var quitChan = make(chan struct{})

func GetQuitChan() chan struct{} {
	return quitChan
}

func CloseQuitChan() {
	close(quitChan)
}
