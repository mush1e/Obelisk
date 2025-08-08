package main

import (
	"bufio"
	"log"
	"net"
	"time"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/pkg/protocol"
)

func main() {
	addr := "localhost:8080"
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal("could not dial " + addr)
	}
	writer := bufio.NewWriter(conn)
	msg := message.Message{
		Timestamp: time.Now(),
		Key:       "user123",
		Value:     "Hello Obelisk :D",
	}
	msgBytes, err := message.Serialize(msg)
	if err != nil {
		log.Fatal(err.Error())
	}
	if err := protocol.WriteMessage(writer, msgBytes); err != nil {
		log.Fatal(err.Error())
	}
}
