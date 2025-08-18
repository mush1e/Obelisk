package server

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mush1e/obelisk/internal/batch"
	"github.com/mush1e/obelisk/internal/buffer"
	"github.com/mush1e/obelisk/internal/config"
	"github.com/mush1e/obelisk/internal/services"
	"github.com/mush1e/obelisk/internal/storage"

	obeliskErrors "github.com/mush1e/obelisk/internal/errors"
)

// Server orchestrates the complete Obelisk message broker infrastructure.
// This is the top-level component that coordinates all subsystems and manages
// their lifecycle from startup through shutdown. The server implements a
// dual-interface architecture with TCP for high-performance message ingestion
// and HTTP for administrative and monitoring operations.
//
// The server manages four primary subsystems:
// 1. TCP server - Handles high-throughput message ingestion from producers
// 2. HTTP server - Provides REST API for monitoring and administrative tasks
// 3. Topic batcher - Manages efficient persistent storage of messages
// 4. Topic buffers - Provides fast in-memory access to recent messages
//
// All subsystems are initialized with consistent configuration and coordinated
// through proper startup/shutdown sequencing to ensure data integrity and
// graceful operation under all conditions.
type Server struct {
	tcpServer     *TCPServer
	httpServer    *HTTPServer
	batcher       *batch.TopicBatcher
	topicBuffers  *buffer.TopicBuffers
	pool          *storage.FilePool
	brokerService *services.BrokerService
	wg            sync.WaitGroup
}

// NewServer creates a new server with TCP/HTTP addresses and storage path. [DEPRECATED]
func NewServer(tcpAddr, httpAddr, logFilePath string) *Server {
	pool := storage.NewFilePool(time.Hour)
	pool.StartCleanup(time.Minute * 2)

	cfg, err := config.LoadConfig("")
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	topicBuffers := buffer.NewTopicBuffers(100)
	brokerService := services.NewBrokerService(topicBuffers, nil) // Will be updated after batcher creation
	batcher := batch.NewTopicBatcher(logFilePath, 100, time.Second*5, pool, brokerService.GetHealthTracker(), cfg)
	brokerService.SetBatcher(batcher) // Update the broker service with the batcher
	tcpServer := NewTCPServer(tcpAddr, brokerService)
	httpServer := NewHTTPServer(httpAddr, brokerService)

	return &Server{
		tcpServer:     tcpServer,
		httpServer:    httpServer,
		batcher:       batcher,
		topicBuffers:  topicBuffers,
		pool:          pool,
		brokerService: brokerService,
	}
}

// NewServerWithConfig creates a server using configuration
func NewServerWithConfig(cfg *config.Config) *Server {
	pool := storage.NewFilePool(cfg.Storage.FilePoolTimeout)
	pool.StartCleanup(cfg.Storage.CleanupInterval)

	topicBuffers := buffer.NewTopicBuffers(100)
	brokerService := services.NewBrokerService(topicBuffers, nil)

	batcher := batch.NewTopicBatcher(
		cfg.Storage.DataDir,
		cfg.Storage.BatchSize,
		cfg.Storage.FlushInterval,
		pool,
		brokerService.GetHealthTracker(),
		cfg,
	)

	brokerService.SetBatcher(batcher)
	tcpServer := NewTCPServer(cfg.Server.TCPAddr, brokerService)
	httpServer := NewHTTPServer(cfg.Server.HTTPAddr, brokerService)

	return &Server{
		tcpServer:     tcpServer,
		httpServer:    httpServer,
		batcher:       batcher,
		topicBuffers:  topicBuffers,
		pool:          pool,
		brokerService: brokerService,
	}
}

// Start starts all subsystems in proper order: batcher, TCP server, HTTP server.
func (s *Server) Start() error {
	if err := s.batcher.Start(); err != nil {
		return obeliskErrors.NewConfigurationError("start_server", "failed to start batcher", err)
	}

	if err := s.tcpServer.Start(); err != nil {
		return obeliskErrors.NewConfigurationError("start_server", "failed to start TCP server", err)
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.httpServer.Start()
	}()

	// Mark the system as initialized after all components are started
	s.brokerService.SetInitialized()

	return nil
}

// Stop shuts down all subsystems in reverse order, ensuring clean shutdown.
func (s *Server) Stop() {
	s.tcpServer.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.httpServer.Stop(ctx)

	s.batcher.Stop()

	if err := s.pool.Stop(); err != nil {
		fmt.Printf("Error shutting down file pool: %v\n", err)
	}

	s.wg.Wait()
	fmt.Println("Server stopped")
}
