package utils

import "sync"

type BoolFlag struct {
	sync.Mutex
	flag bool
}

func (this *BoolFlag) SetFalse() {
	this.Lock()
	this.flag = false
	this.Unlock()
}

func (this *BoolFlag) SetTrue() {
	this.Lock()
	this.flag = true
	this.Unlock()
}

func (this *BoolFlag) IsTrue() bool {
	return this.flag
}

func (this *BoolFlag) IsFalse() bool {
	return !this.flag
}
