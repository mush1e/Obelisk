package server

import (
	"bufio"
	"fmt"
	"net"
	"sync"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/pkg/protocol"
)

// TCPServer handles incoming TCP connections and delegates message processing
type TCPServer struct {
	address  string
	listener net.Listener
	quit     chan struct{}
	wg       sync.WaitGroup
	handler  ConnectionHandler
}

type ConnectionHandler interface {
	HandleMessage(msg *message.Message) error
}

func NewTCPServer(address string, handler ConnectionHandler) *TCPServer {
	return &TCPServer{
		address: address,
		quit:    make(chan struct{}),
		handler: handler,
	}
}

func (t *TCPServer) Start() error {
	var err error
	t.listener, err = net.Listen("tcp", t.address)
	if err != nil {
		return fmt.Errorf("failed to start TCP listener: %w", err)
	}
	fmt.Println("TCP server started on", t.address)

	t.wg.Add(1)
	go t.acceptLoop()
	return nil
}

func (t *TCPServer) Stop() {
	close(t.quit)

	if t.listener != nil {
		t.listener.Close()
	}

	t.wg.Wait()
	fmt.Println("TCP server stopped")
}

func (t *TCPServer) acceptLoop() {
	defer t.wg.Done()

	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.quit:
				return // shutting down
			default:
				fmt.Println("Accept error:", err)
				continue
			}
		}

		fmt.Println("New client connected:", conn.RemoteAddr())

		t.wg.Add(1)
		go t.handleConnection(conn)
	}
}

func (t *TCPServer) handleConnection(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		select {
		case <-t.quit:
			fmt.Println("Closing connection:", conn.RemoteAddr())
			return
		default:
			msgBytes, err := protocol.ReadMessage(reader)
			if err != nil {
				if err.Error() == "EOF" {
					fmt.Println("Client disconnected:", conn.RemoteAddr())
				} else {
					fmt.Println("Error receiving message from", conn.RemoteAddr(), ":", err)
				}
				return
			}

			msg, err := message.Deserialize(msgBytes)
			if err != nil {
				fmt.Println("Invalid message format:", err)
				continue
			}

			if err := t.handler.HandleMessage(&msg); err != nil {
				fmt.Printf("Error handling message: %v\n", err)
			}

			// Send acknowledgement
			conn.Write([]byte("OK\n"))
		}
	}
}
