package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mush1e/obelisk/internal/config"
	"github.com/mush1e/obelisk/internal/server"
	"github.com/mush1e/obelisk/internal/storage"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "", "Path to config file (optional)")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Printf("Invalid config: %v\n", err)
		os.Exit(1)
	}

	logPath, idxPath := storage.GetPartitionedPaths("data/topics", "orders", 2)
	fmt.Printf("Partition paths: %s, %s\n", logPath, idxPath)

	// Graceful shutdown
	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGINT, syscall.SIGTERM)

	// Create server with config
	srv := server.NewServerWithConfig(cfg)

	if err := srv.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		return
	}

	fmt.Printf("Obelisk started!\n")
	fmt.Printf("TCP server: %s\n", cfg.Server.TCPAddr)
	fmt.Printf("HTTP server: %s\n", cfg.Server.HTTPAddr)
	if cfg.Metrics.Enabled {
		fmt.Printf("Metrics: http://%s%s\n",
			cfg.Server.HTTPAddr[1:], cfg.Metrics.Path)
	}

	<-gracefulShutdown

	fmt.Println("\nServer shutting down...")
	srv.Stop()
}
