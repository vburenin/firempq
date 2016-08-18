package apis

// IServer is a server interface that can be started or stopped.
type IServer interface {
	Start()
	Stop()
}
