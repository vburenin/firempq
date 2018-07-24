package client

import (
	"log"
	"net"
	"time"
)

var _ net.Conn = &NetConnDebug{}

type NetConnDebug struct {
	conn net.Conn
}

func (nc *NetConnDebug) Read(b []byte) (n int, err error) {
	n, err = nc.conn.Read(b)
	if err != nil {
		log.Printf("read error: %s", err)
	}
	if nc != nil {
		log.Printf("read (%d bytes): %s", n, string(b[:n]))
	}
	return n, err
}

func (nc *NetConnDebug) Write(b []byte) (n int, err error) {
	n, err = nc.conn.Write(b)
	if err != nil {
		log.Printf("write error: %s", err)
	}
	if nc != nil {
		log.Printf("write (%d bytes): %s", n, string(b))
	}
	return n, err
}

func (nc *NetConnDebug) Close() error {
	log.Println("closed")
	return nc.conn.Close()
}

func (nc *NetConnDebug) LocalAddr() net.Addr {
	return nc.conn.LocalAddr()
}

func (nc *NetConnDebug) RemoteAddr() net.Addr {
	return nc.conn.RemoteAddr()
}

func (nc *NetConnDebug) SetDeadline(t time.Time) error {
	return nc.conn.SetDeadline(t)
}

func (nc *NetConnDebug) SetReadDeadline(t time.Time) error {
	return nc.conn.SetReadDeadline(t)
}

func (nc *NetConnDebug) SetWriteDeadline(t time.Time) error {
	return nc.conn.SetWriteDeadline(t)
}
