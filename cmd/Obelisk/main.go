// Package main provides the entry point for the Obelisk message broker server.
// It sets up graceful shutdown handling and starts the TCP server on port 8080.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mush1e/obelisk/internal/server"
)

// main initializes and starts the Obelisk server with graceful shutdown handling.
// The server listens on port 8080 and stores topic data in the "data/topics/" directory.
func main() {
	// Set up channel to listen for interrupt signals for graceful shutdown
	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGINT, syscall.SIGTERM)

	logFilePath := "data/topics/"

	srv := server.NewServer(":8080", logFilePath)
	if err := srv.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		return
	}
	fmt.Println("Server running. Press Ctrl+C to stop...")

	// Block until we receive a shutdown signal
	<-gracefulShutdown

	fmt.Println("\nServer shutting down!")
	if err := srv.Stop(); err != nil {
		fmt.Println("error while graceful shutdown : ", err)
	}
}
