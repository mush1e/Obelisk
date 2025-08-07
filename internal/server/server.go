package server

// This package provides a server implementation for the Obelisk application.
// It includes the Server struct and its associated methods for managing the server's state and behavior.

import (
	"bufio"
	"fmt"
	"net"
	"sync"
)

// Server holds TCP server state
type Server struct {
	address  string
	listener net.Listener
	wg       sync.WaitGroup
	quit     chan struct{}
}

// NewServer creates a new Server instance
func NewServer(address string) *Server {
	return &Server{
		address: address,
		quit:    make(chan struct{}),
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
			message, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Client disconnected:", conn.RemoteAddr())
				return
			}

			fmt.Printf("Received: %s", message)
			conn.Write([]byte("Echo: " + message))
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

	s.wg.Wait()
	fmt.Println("Server stopped")
	return nil
}
