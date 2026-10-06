// Package grpcserver bootstraps a gRPC server with sensible defaults.
//
// A server is built with functional options, services are attached with
// [Server.Register], and [Server.Run] serves until its context is cancelled:
//
//	srv := grpcserver.New(
//		grpcserver.WithAddress(":50051"),
//		grpcserver.WithLogger(logger),
//		grpcserver.WithValidation(),
//		grpcserver.WithHealth(),
//	)
//	srv.Register(func(s *grpc.Server) { pb.RegisterGreeterServer(s, impl) })
//
//	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
//	defer stop()
//	if err := srv.Run(ctx); err != nil { ... }
//
// Every call passes through this interceptor chain, outermost first:
//
//	logging → panic recovery → error mapping → user interceptors → validation → handler
//
// Logging sees the final status code. Recovery turns panics anywhere below
// it into codes.Internal. Error mapping (see the grpcerr package) converts
// errs kinds returned by handlers, validation and user interceptors into
// gRPC statuses.
package grpcserver

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/UsatovPavel/go-grpc-kit/grpcerr"
)

// Server is a gRPC server with a managed lifecycle. Create it with [New].
type Server struct {
	cfg    config
	grpc   *grpc.Server
	health *health.Server
}

// New builds a server from the given options. Services are added with
// [Server.Register]; nothing listens until [Server.Run] is called.
func New(opts ...Option) *Server {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	unary := []grpc.UnaryServerInterceptor{
		unaryLogging(cfg.logger),
		unaryRecovery(cfg.logger),
		grpcerr.UnaryServerInterceptor(),
	}
	unary = append(unary, cfg.unary...)
	stream := []grpc.StreamServerInterceptor{
		streamLogging(cfg.logger),
		streamRecovery(cfg.logger),
		grpcerr.StreamServerInterceptor(),
	}
	stream = append(stream, cfg.stream...)
	if cfg.validator != nil {
		unary = append(unary, unaryValidation(cfg.validator))
		stream = append(stream, streamValidation(cfg.validator))
	}

	serverOpts := append([]grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(stream...),
	}, cfg.grpcServerOptions...)

	s := &Server{cfg: cfg, grpc: grpc.NewServer(serverOpts...)}
	if cfg.health {
		s.health = health.NewServer()
		healthpb.RegisterHealthServer(s.grpc, s.health)
	}
	if cfg.reflection {
		reflection.Register(s.grpc)
	}
	return s
}

// Register calls fn with the underlying *grpc.Server so that generated
// RegisterXxxServer functions can attach services. It must be called before
// [Server.Run]. It returns s to allow chaining.
func (s *Server) Register(fn func(*grpc.Server)) *Server {
	fn(s.grpc)
	return s
}

// Health returns the health service registered by [WithHealth], or nil if
// health checking is disabled. Use it to report the status of individual
// services.
func (s *Server) Health() *health.Server {
	return s.health
}

// Run starts serving and blocks until ctx is cancelled or the server fails.
//
// On cancellation it marks the health service NOT_SERVING, stops accepting
// new connections and waits for in-flight calls to finish. If they do not
// finish within the shutdown timeout (see [WithShutdownTimeout]), all
// connections are closed forcibly. Run returns nil after a shutdown caused
// by ctx, and an error if the server could not listen or stopped serving
// on its own. Run must be called at most once.
func (s *Server) Run(ctx context.Context) error {
	lis := s.cfg.listener
	if lis == nil {
		var err error
		lis, err = (&net.ListenConfig{}).Listen(ctx, "tcp", s.cfg.address)
		if err != nil {
			return fmt.Errorf("grpcserver: listen on %s: %w", s.cfg.address, err)
		}
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- s.grpc.Serve(lis) }()
	s.cfg.logger.InfoContext(ctx, "grpc server started", "addr", lis.Addr().String())

	select {
	case err := <-serveErr:
		return fmt.Errorf("grpcserver: serve: %w", err)
	case <-ctx.Done():
	}

	s.cfg.logger.InfoContext(ctx, "grpc server shutting down")
	s.shutdown(ctx)
	<-serveErr // Serve returns nil once the server is stopped.
	s.cfg.logger.InfoContext(ctx, "grpc server stopped")
	return nil
}

// shutdown stops the server gracefully, falling back to a hard stop after
// the configured timeout. ctx is used for logging only.
func (s *Server) shutdown(ctx context.Context) {
	if s.health != nil {
		s.health.Shutdown()
	}

	done := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(done)
	}()

	if s.cfg.shutdownTimeout <= 0 {
		<-done
		return
	}
	timer := time.NewTimer(s.cfg.shutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		s.cfg.logger.WarnContext(ctx, "graceful shutdown timed out, forcing stop",
			"timeout", s.cfg.shutdownTimeout)
		s.grpc.Stop()
		<-done
	}
}
