// github.com/illmade-knight/go-iot-dataflows/builder/service.go
package microservice

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/rs/zerolog"
)

// BaseConfig holds common configuration fields for all services.
type BaseConfig struct {
	LogLevel        string `mapstructure:"log_level"`
	HTTPPort        string `mapstructure:"http_port"`
	ProjectID       string `mapstructure:"project_id"`
	CredentialsFile string `mapstructure:"credentials_file"`
}

// Service defines the common interface for all microservices.
type Service interface {
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
	Mux() *http.ServeMux
	GetHTTPPort() string
}

// BaseServer provides common functionalities for microservice servers.
type BaseServer struct {
	Logger     zerolog.Logger
	HTTPPort   string
	httpServer *http.Server
	mux        *http.ServeMux
	actualAddr string
	mu         sync.RWMutex
}

// NewBaseServer creates and initializes a new BaseServer.
func NewBaseServer(logger zerolog.Logger, httpPort string) *BaseServer {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", HealthzHandler)

	return &BaseServer{
		Logger:   logger,
		HTTPPort: httpPort,
		mux:      mux,
		httpServer: &http.Server{
			Addr:    httpPort,
			Handler: mux,
		},
	}
}

// Start initiates the HTTP server in a background goroutine.
func (s *BaseServer) Start() error {
	listener, err := net.Listen("tcp", s.HTTPPort)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", s.HTTPPort, err)
	}

	s.mu.Lock()
	s.actualAddr = listener.Addr().String()
	s.mu.Unlock()

	s.Logger.Info().Str("address", s.actualAddr).Msg("HTTP server starting to listen")

	go func() {
		if err := s.httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			s.Logger.Error().Err(err).Msg("HTTP server failed")
		}
	}()

	return nil
}

// Shutdown gracefully stops the HTTP server, respecting the provided context's deadline.
// REFACTOR: The Shutdown method now accepts a context.
func (s *BaseServer) Shutdown(ctx context.Context) error {
	s.Logger.Info().Msg("Shutting down HTTP server...")
	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.Logger.Error().Err(err).Msg("Error during HTTP server shutdown.")
		return err
	}
	s.Logger.Info().Msg("HTTP server stopped.")
	return nil
}

// GetHTTPPort returns the actual configured HTTP port the server is listening on.
func (s *BaseServer) GetHTTPPort() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, port, err := net.SplitHostPort(s.actualAddr)
	if err != nil {
		return s.HTTPPort
	}
	return ":" + port
}

// Mux returns the underlying ServeMux.
func (s *BaseServer) Mux() *http.ServeMux {
	return s.mux
}

// HealthzHandler responds to health check probes.
func HealthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}
