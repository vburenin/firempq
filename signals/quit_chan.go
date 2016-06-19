package signals

var QuitChan = make(chan struct{})

// CloseQuitChan sends a global signal that service is about to shutdown.
func CloseQuitChan() {
	close(QuitChan)
}
