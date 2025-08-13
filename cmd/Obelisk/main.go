// Obelisk message broker server entry point.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mush1e/obelisk/internal/server"
)

func main() {
	// Graceful shutdown handling
	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGINT, syscall.SIGTERM)

	logFilePath := "data/topics/"

	srv := server.NewServer(":8080", ":8081", logFilePath)

	if err := srv.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		return
	}
	fmt.Println("Server running. Press Ctrl+C to stop...")

	<-gracefulShutdown

	fmt.Println("\nServer shutting down!")
	srv.Stop()
}
