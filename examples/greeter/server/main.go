// Command server runs the example greeter service.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/UsatovPavel/go-grpc-kit/examples/greeter"
	greeterv1 "github.com/UsatovPavel/go-grpc-kit/examples/greeter/greeterpb/greeter/v1"
	"github.com/UsatovPavel/go-grpc-kit/grpcserver"
)

func main() {
	addr := flag.String("addr", ":50051", "listen address")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	srv := grpcserver.New(
		grpcserver.WithAddress(*addr),
		grpcserver.WithLogger(logger),
		grpcserver.WithValidation(),
		grpcserver.WithReflection(),
		grpcserver.WithHealth(),
		grpcserver.WithShutdownTimeout(5*time.Second),
	)
	srv.Register(func(s *grpc.Server) {
		greeterv1.RegisterGreeterServiceServer(s, greeter.Service{})
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := srv.Run(ctx)
	stop()
	if err != nil {
		logger.Error("server failed", "err", err)
		os.Exit(1)
	}
}
