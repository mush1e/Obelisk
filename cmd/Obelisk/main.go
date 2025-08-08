package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mush1e/obelisk/internal/server"
)

func main() {
	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGINT, syscall.SIGTERM)

	srv := server.NewServer(":8080")
	if err := srv.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		return
	}
	fmt.Println("Server running. Press Ctrl+C to stop...")

	<-gracefulShutdown

	fmt.Println("\nServer shutting down!")
	if err := srv.Stop(); err != nil {
		fmt.Println("error while graceful shutdown : ", err)
	}
}
