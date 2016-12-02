package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"
	"os"
)

const UDP = "udp"

type Event map[string]interface{}

var (
	In     = make(chan Event, 10000)
	events = make([]Event, 0)
)

func store(buf *bytes.Buffer) {
	var event Event
	err := json.Unmarshal(buf.Bytes(), &event)
	if err != nil {
		fmt.Println("error parsing event")
	}
	In <- event
}

type BulkUploadRequest struct {
	 Events []Event  `json:"events"`
}

func flush() {
	num_events := len(events)
	if num_events < 1 {
		return
	}
	fmt.Printf("flushing %d events\n", num_events)

	body := new(bytes.Buffer)
	json.NewEncoder(body).Encode(BulkUploadRequest { Events: events })
	_, err := http.Post(os.Getenv("COLLECTOR"), "application/json", body)
	if err != nil {
		fmt.Println(err.Error())
	}
	events = []Event{}
}

func run() {
	t := time.NewTicker(time.Duration(10) * time.Second)

	for {
		select {
		case <-t.C:
			flush()
		case s := <-In:
			events = append(events, s)
		}
	}
}

func process(conn *net.UDPConn) {
	var buf [2048]byte
	n, err := conn.Read(buf[0:])
	if err != nil {
		fmt.Println("error reading from udp socket")
		return
	}

	go store(bytes.NewBuffer(buf[:n]))
}

func startListener() {
	address, _ := net.ResolveUDPAddr(UDP, ":8015")
	listener, err := net.ListenUDP(UDP, address)
	if err != nil {
		fmt.Printf("error listening on udp socket %s", err)
	}
	for {
		process(listener)
	}
}

func main() {
	go startListener()
	run()
}
