package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/mush1e/obelisk/internal/handlers"
	"github.com/mush1e/obelisk/internal/services"
)

type HTTPServer struct {
	srv *http.Server
}

func NewHTTPServer(addr string, topicService *services.TopicService) *HTTPServer {
	mux := http.NewServeMux()

	mux.Handle("/health", handlers.Health())
	mux.Handle("/stats", handlers.TopicStats(topicService))

	return &HTTPServer{
		srv: &http.Server{
			Addr:    addr,
			Handler: handlers.WithMiddlewares(mux),
		},
	}
}

func (h *HTTPServer) Start() {
	fmt.Println("HTTP server started on", h.srv.Addr)
	if err := h.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Println("HTTP server error:", err)
	}
}

func (h *HTTPServer) Stop(ctx context.Context) error {
	return h.srv.Shutdown(ctx)
}
