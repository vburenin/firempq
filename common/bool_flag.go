package common
import "sync"


type BoolFlag struct {
	sync.Mutex
	Flag bool
}

func (this *BoolFlag) SetFalse() {
	this.Lock()
	this.Flag = false
	this.Unlock()
}

func (this *BoolFlag) SetTrue() {
	this.Lock()
	this.Flag = true
	this.Unlock()
}
