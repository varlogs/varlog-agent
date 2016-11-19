package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"
)

const UDP = "udp"

var (
	In     = make(chan string, 10000)
	events = make([]string, 0)
)

func store(buf bytes.Buffer) {
	In <- buf.String()
}

func flush() {
	body := new(bytes.Buffer)
	json.NewEncoder(body).Encode(events)
	_, err := http.Post("http://localhost:4567/events", "application/json", body)
	if err != nil {
		fmt.Println(err.Error())
	}
	events = []string{}
}

func run() {
	t := time.NewTicker(time.Duration(10) * time.Second)

	for {
		select {
		case <-t.C:
			fmt.Println("Timer executing")
			flush()
		case s := <-In:
			fmt.Printf("Buffering: %s", s)
			events = append(events, s)
			fmt.Printf("num of events %d", len(events))
		}
	}
}

func process(conn *net.UDPConn) {
	var buf [2048]byte
	n, err := conn.Read(buf[0:])
	if err != nil {
		fmt.Println("Error Reading")
		return
	}

	go store(bytes.NewBuffer(buf[:n]))
}

func startListener() {
	address, _ := net.ResolveUDPAddr(UDP, ":8015")
	listener, err := net.ListenUDP(UDP, address)
	if err != nil {
		fmt.Printf("Error listening on UDP %s", err)
	}
	for {
		process(listener)
	}
}

func main() {
	go startListener()
	run()
}
