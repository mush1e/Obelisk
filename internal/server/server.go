package server

// This package provides a server implementation for the Obelisk application.
// It includes the Server struct and its associated methods for managing the server's state and behavior.

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/mush1e/obelisk/internal/batch"
	"github.com/mush1e/obelisk/internal/buffer"
	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/pkg/protocol"
)

// Server holds TCP server state
type Server struct {
	address  string
	listener net.Listener
	wg       sync.WaitGroup
	quit     chan struct{}

	// storage stuff
	topicBuffers *buffer.TopicBuffers
	batcher      *batch.Batcher
}

// NewServer creates a new Server instance
func NewServer(address, logFilePath string) *Server {
	return &Server{
		address:      address,
		quit:         make(chan struct{}),
		topicBuffers: buffer.NewTopicBuffers(10),
		batcher:      batch.NewBatcher(logFilePath, 100, time.Second*5),
	}
}

// Start begins listening and accepting TCP connections
func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}

	fmt.Println("Server started on", s.address)

	s.wg.Add(1)
	go s.acceptLoop()
	s.batcher.Start()
	return nil
}

// acceptLoop waits for and handles new connections
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return // shutting down
			default:
				fmt.Println("Accept error:", err)
				continue
			}
		}

		fmt.Println("New client connected:", conn.RemoteAddr())

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection processes data from a single client
func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		select {
		case <-s.quit:
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

			fmt.Printf("Received message - Key: %s, Value: %s\n", msg.Key, msg.Value)
			s.topicBuffers.Push(msg)  // For fast reads
			s.batcher.AddMessage(msg) // For batched storage
			// Send simple response back
			response := []byte("OK\n")
			conn.Write(response)
		}
	}
}

// Stop gracefully shuts down the server
func (s *Server) Stop() error {
	close(s.quit)

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("failed to close listener: %w", err)
		}
	}
	s.batcher.Stop()
	s.wg.Wait()
	fmt.Println("Server stopped")
	return nil
}
