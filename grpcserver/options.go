package grpcserver

import (
	"log/slog"
	"net"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
)

// DefaultAddress is the address the server listens on when neither
// [WithAddress] nor [WithListener] is given.
const DefaultAddress = ":50051"

// DefaultShutdownTimeout is how long [Server.Run] waits for in-flight calls
// to finish before forcing the server to stop.
const DefaultShutdownTimeout = 10 * time.Second

// Option configures a [Server].
type Option func(*config)

type config struct {
	address           string
	listener          net.Listener
	logger            *slog.Logger
	unary             []grpc.UnaryServerInterceptor
	stream            []grpc.StreamServerInterceptor
	validator         protovalidate.Validator
	reflection        bool
	health            bool
	shutdownTimeout   time.Duration
	grpcServerOptions []grpc.ServerOption
}

func defaultConfig() config {
	return config{
		address:         DefaultAddress,
		logger:          slog.New(slog.DiscardHandler),
		shutdownTimeout: DefaultShutdownTimeout,
	}
}

// WithAddress sets the TCP address to listen on, such as ":8080" or
// "127.0.0.1:0". It is ignored when [WithListener] is used.
func WithAddress(addr string) Option {
	return func(c *config) { c.address = addr }
}

// WithListener makes the server accept connections on lis instead of
// opening its own TCP listener. It is mostly useful in tests, for example
// with google.golang.org/grpc/test/bufconn. The server closes lis when it
// stops.
func WithListener(lis net.Listener) Option {
	return func(c *config) { c.listener = lis }
}

// WithLogger sets the logger used for lifecycle events, call logs and
// recovered panics. By default nothing is logged. A nil logger is ignored.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithInterceptors appends unary interceptors to the chain. They run after
// the built-in logging, recovery and error-mapping interceptors and before
// validation, so errors they return are mapped to statuses and panics they
// raise are recovered. Interceptors run in the order given.
func WithInterceptors(interceptors ...grpc.UnaryServerInterceptor) Option {
	return func(c *config) { c.unary = append(c.unary, interceptors...) }
}

// WithStreamInterceptors is the streaming counterpart of [WithInterceptors].
func WithStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) Option {
	return func(c *config) { c.stream = append(c.stream, interceptors...) }
}

// WithValidation enables request validation with protovalidate
// (buf.build/go/protovalidate) using its global validator. Requests that
// violate their rules are rejected with codes.InvalidArgument before they
// reach the handler.
func WithValidation() Option {
	return WithValidator(protovalidate.GlobalValidator)
}

// WithValidator is like [WithValidation] but uses the given validator,
// which allows custom protovalidate options. A nil validator disables
// validation.
func WithValidator(v protovalidate.Validator) Option {
	return func(c *config) { c.validator = v }
}

// WithReflection registers the gRPC server reflection service, so tools
// such as grpcurl can discover the API.
func WithReflection() Option {
	return func(c *config) { c.reflection = true }
}

// WithHealth registers the standard gRPC health service
// (grpc.health.v1.Health). It reports SERVING while the server runs and
// NOT_SERVING once shutdown begins. Use [Server.Health] to set the status of
// individual services.
func WithHealth() Option {
	return func(c *config) { c.health = true }
}

// WithShutdownTimeout sets how long [Server.Run] waits for in-flight calls
// after its context is cancelled before it forcibly closes all
// connections. A value of zero or less waits indefinitely.
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *config) { c.shutdownTimeout = d }
}

// WithGRPCOptions passes additional options to grpc.NewServer, for example
// credentials or keepalive parameters. Interceptor options given here are
// chained after the ones configured by this package.
func WithGRPCOptions(opts ...grpc.ServerOption) Option {
	return func(c *config) { c.grpcServerOptions = append(c.grpcServerOptions, opts...) }
}
