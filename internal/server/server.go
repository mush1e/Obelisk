package server

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
	batcher      *batch.TopicBatcher
}

// NewServer creates a new Server instance
func NewServer(address, logFilePath string) *Server {
	return &Server{
		address:      address,
		quit:         make(chan struct{}),
		topicBuffers: buffer.NewTopicBuffers(100), // Increased buffer size
		batcher:      batch.NewTopicBatcher(logFilePath, 100, time.Second*5),
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

	// Start the batcher
	if err := s.batcher.Start(); err != nil {
		return fmt.Errorf("failed to start batcher: %w", err)
	}

	s.wg.Add(1)
	go s.acceptLoop()
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

			fmt.Printf("Received message - Topic: %s, Key: %s, Value: %s\n", msg.Topic, msg.Key, msg.Value)

			// Store in memory buffer for fast reads
			s.topicBuffers.Push(msg)

			// Add to batcher for persistent storage with indexing
			if err := s.batcher.AddMessage(msg); err != nil {
				fmt.Printf("Error adding message to batcher: %v\n", err)
			}

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

// GetTopicStats returns statistics for a specific topic
func (s *Server) GetTopicStats(topic string) (buffered int, persisted int64, err error) {
	return s.batcher.GetTopicStats(topic)
}
