package common

var quitChan = make(chan struct{})

// GetQuitChan returns globally used channel to signal service shutdown.
func GetQuitChan() chan struct{} {
	return quitChan
}

// CloseQuitChan sends a global signal that service is about to shutdown.
func CloseQuitChan() {
	close(quitChan)
}
