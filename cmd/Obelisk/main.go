// Obelisk message broker server entry point.
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

func main() {
	// Graceful shutdown handling
	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGINT, syscall.SIGTERM)

	storage.InitializePool(time.Hour, time.Minute*10)

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
	if err := storage.ShutdownPool(); err != nil {
		fmt.Println("error shutting down storage pool : " + err.Error())
	}
}
