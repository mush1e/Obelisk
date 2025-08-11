// Package main provides the entry point for the Obelisk message broker server.
// It sets up graceful shutdown handling and starts the TCP server on port 8080.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mush1e/obelisk/internal/server"
	"github.com/mush1e/obelisk/internal/storage"
)

// main initializes and starts the Obelisk server with graceful shutdown handling.
// The server listens on port 8080 and stores topic data in the "data/topics/" directory.
func main() {
	// Set up channel to listen for interrupt signals for graceful shutdown
	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGINT, syscall.SIGTERM)

	// Initialize storage pool before starting server
	storage.InitializePool(time.Hour, time.Minute*10)

	logFilePath := "data/topics/"

	srv := server.NewServer(":8080", ":8081", logFilePath)

	if err := srv.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		return
	}
	fmt.Println("Server running. Press Ctrl+C to stop...")

	// Block until we receive a shutdown signal
	<-gracefulShutdown

	fmt.Println("\nServer shutting down!")
	srv.Stop()
	if err := storage.ShutdownPool(); err != nil {
		fmt.Println("error shutting down storage pool : " + err.Error())
	}
}
