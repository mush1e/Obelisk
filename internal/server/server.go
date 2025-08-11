package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mush1e/obelisk/internal/batch"
	"github.com/mush1e/obelisk/internal/buffer"
	"github.com/mush1e/obelisk/internal/services"
)

// Server orchestrates TCP server, HTTP server, and background services
type Server struct {
	tcpServer    *TCPServer
	httpServer   *HTTPServer
	batcher      *batch.TopicBatcher
	topicBuffers *buffer.TopicBuffers
	wg           sync.WaitGroup
}

func NewServer(tcpAddr, httpAddr, logFilePath string) *Server {
	topicBuffers := buffer.NewTopicBuffers(100)
	batcher := batch.NewTopicBatcher(logFilePath, 100, time.Second*5)

	// Connection handler for TCP
	tcpHandler := NewObeliskConnectionHandler(topicBuffers, batcher)
	tcpServer := NewTCPServer(tcpAddr, tcpHandler)

	// HTTP service
	topicService := services.NewTopicService(batcher)
	httpServer := NewHTTPServer(httpAddr, topicService)

	return &Server{
		tcpServer:    tcpServer,
		httpServer:   httpServer,
		batcher:      batcher,
		topicBuffers: topicBuffers,
	}
}

func (s *Server) Start() error {
	// Start batcher
	if err := s.batcher.Start(); err != nil {
		return fmt.Errorf("failed to start batcher: %w", err)
	}

	// Start TCP server
	if err := s.tcpServer.Start(); err != nil {
		return fmt.Errorf("failed to start TCP server: %w", err)
	}

	// Start HTTP server in goroutine
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.httpServer.Start()
	}()

	return nil
}

func (s *Server) Stop() {
	// Stop TCP
	s.tcpServer.Stop()

	// Stop HTTP
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.httpServer.Stop(ctx)

	// Stop batcher
	s.batcher.Stop()

	s.wg.Wait()
	fmt.Println("Server stopped")
}
