package idx_heap

import (
    "testing"
    "time"
)

func TestIntHeap1(t *testing.T) {
    println("*************")
    intHeap := IntHeap{}

    st := time.Now().UnixNano()

    var i int
    for i = 0; i < 20000000; i++ {
        intHeap.PushItem(i)
    }

    e_time := time.Now().UnixNano()
    println((e_time - st) / 1000000)
    println("--------")

    st = time.Now().UnixNano()

    kk := 0
    for intHeap.Len() > 0 {
        intHeap.PopItem()
        kk += 1
    }
    e_time = time.Now().UnixNano()
    println((e_time - st) / 1000000)
    println("--------")
    println(kk)
}
