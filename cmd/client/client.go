package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"os"
	"strconv"

	"github.com/vburenin/firempq/export/client"
)

var last_ts int64
var lck sync.Mutex
var counter int64

const MSG_T = 20000

func inc() {
	lck.Lock()
	counter++
	if counter >= MSG_T {
		counter = 0
		prev_ts := last_ts
		last_ts = time.Now().UnixNano()
		lck.Unlock()

		tdelta := float64(last_ts-prev_ts) / 1000000000.0

		fmt.Println(MSG_T / tdelta)

	} else {
		lck.Unlock()

	}

}

var m sync.Mutex
var l int64
var st int64 = time.Now().UnixNano()

const LOOP_C = 1000000

func cnt(num int64) {
	m.Lock()
	l += num
	if l == LOOP_C {
		et := time.Now().UnixNano()
		d := float64(et-st) / 1000000000
		println(int64(LOOP_C / d))
		l = 0
		st = time.Now().UnixNano()
	}
	m.Unlock()
}

func sendTwo(addr, qname string) {
	c, err := client.NewFireMpqClient("tcp", addr)
	if err != nil {
		log.Fatal(err.Error())
	}

	c.CreateQueue(qname, nil)

	pq, err := c.Queue(qname)
	if err != nil {
		log.Fatal(err.Error())
	}
	//v := pq.SetParams(NewPQueueOptions().SetDelay(0).SetPopLimit(10).SetMsgTtl(5000))
	//if v != nil {
	//	log.Fatal(v.Error())
	//}
	l := 0
	data := "a"

	l++
	cnt(3)
	msg1 := pq.NewMessage(data).SetId("test1")
	msg2 := pq.NewMessage(data).SetId("test1")
	msg3 := pq.NewMessage(data).SetId("test2")
	resp, err := pq.PushBatch(msg1, msg2, msg3)
	if err != nil {
		log.Fatal(err.Error())
	}
	for _, r := range resp {
		fmt.Printf("%s %s\n", r.MsgID, r.Error)
	}
	/*		msg, err := pq.PopLock(nil)
			if err != nil {
				log.Fatal(err.Error())
			}
			if len(msg) == 0 {
				return
			}
			err = pq.DeleteByReceipt(msg[0].Receipt)
			if err != nil {
				log.Fatal(err.Error())
			}*/

}

func send10messages(addr, qname string) {
	c, err := client.NewFireMpqClient("tcp", addr)
	if err != nil {
		log.Fatal(err.Error())
	}

	c.CreateQueue(qname, nil)

	pq, err := c.Queue(qname)
	if err != nil {
		log.Fatal(err.Error())
	}

	data := "s"
	_, err = pq.PushBatch(
		pq.NewMessage(data), pq.NewMessage(data),
		pq.NewMessage(data), pq.NewMessage(data),
		pq.NewMessage(data), pq.NewMessage(data),
		pq.NewMessage(data), pq.NewMessage(data),
		pq.NewMessage(data), pq.NewMessage(data),
	)
	if err != nil {
		log.Fatal(err.Error())
	}

	_, err = pq.Pop(client.NewPopOptions().SetLimit(10))
	if err != nil {
		log.Fatal(err.Error())
	}

}

func update(addr, qname string) {
	c, err := client.NewFireMpqClient("tcp", addr)
	if err != nil {
		log.Fatal(err.Error())
	}

	c.CreateQueue(qname, nil)

	pq, err := c.Queue(qname)
	if err != nil {
		log.Fatal(err.Error())
	}
	//v := pq.SetParams(NewPQueueOptions().SetDelay(0).SetPopLimit(10).SetMsgTtl(5000))
	//if v != nil {
	//	log.Fatal(v.Error())
	//}
	l := 0
	data := "s"
	for {
		l++
		cnt(10)
		_, err := pq.PushBatch(
			pq.NewMessage(data), pq.NewMessage(data),
			pq.NewMessage(data), pq.NewMessage(data),
			pq.NewMessage(data), pq.NewMessage(data),
			pq.NewMessage(data), pq.NewMessage(data),
			pq.NewMessage(data), pq.NewMessage(data),
		)

		if err != nil {
			log.Fatal(err.Error())
		}

		_, err = pq.Pop(client.NewPopOptions().SetLimit(10))
		if err != nil {
			log.Fatal(err.Error())
		}
		//if len(msg) == 0 {
		//			return
		//		}
	}
}

func main() {
	/*f, _ := os.Create("client.profile")
	defer f.Close()
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()*/
	addr := os.Args[1]
	for i := 0; i < 10; i++ {
		n := "my" + strconv.Itoa(i)
		for j := 0; j < 10; j++ {
			go update(addr, n)
		}
	}
	time.Sleep(time.Second * 30)

	//send10messages(os.Args[1], "testq")
}
