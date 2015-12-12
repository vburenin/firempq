package counters

import (
	"testing"
	"time"
)

func TestAutoDecrement(t *testing.T) {
	cnt := NewIntLinearCounter("id", 1000, 0, 10000, -100.0, 200)

	time.Sleep(time.Millisecond)
	cnt.Update()
	if cnt.Value != 1000 {
		t.Fatal("Value should not be decremented!")
	}

	time.Sleep(time.Millisecond * 10)
	cnt.Update()
	if cnt.Value != 999 {
		t.Fatal("Value should be 999!")
	}

	time.Sleep(time.Millisecond * 10)
	cnt.Update()
	if cnt.Value != 998 {
		t.Fatal("Value should be 998!")
	}
	time.Sleep(time.Millisecond * 100)
	cnt.Update()
	if cnt.Value != 988 {
		t.Fatal("Value should be 988!")
	}
}

func TestAutoDecrement2(t *testing.T) {
	cnt := NewIntLinearCounter("id", 1000, 0, 10000, -100000.0, 200)

	time.Sleep(time.Millisecond)
	cnt.Update()
	if cnt.Value > 900 {
		t.Fatal("Value should be decremented!")
	}

	time.Sleep(time.Millisecond * 20)
	cnt.Update()
	if cnt.Value != 0 {
		t.Fatal("Value should be 0!")
	}
}

func TestLimits(t *testing.T) {
	cnt := NewIntLinearCounter("id", 1000, 0, 10000, -100000.0, 200)

	cnt.Value = 100000000000
	cnt.Update()
	if cnt.Value != 10000 {
		t.Fatal("Value should be 10000!")
	}

	cnt.Value = -100000
	cnt.Update()
	if cnt.Value != 0 {
		t.Fatal("Value should be 0!")
	}
}
