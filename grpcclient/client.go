// Package grpcclient creates gRPC client connections with sensible
// defaults.
//
//	conn, err := grpcclient.Dial("localhost:50051",
//		grpcclient.WithInsecure(),
//		grpcclient.WithCallTimeout(2*time.Second),
//		grpcclient.WithErrorMapping(),
//	)
//	if err != nil { ... }
//	defer conn.Close()
//	client := pb.NewGreeterClient(conn)
//
// Unary calls pass through these interceptors, outermost first:
//
//	error mapping → logging → call timeout → user interceptors → transport
//
// so the logger sees raw gRPC statuses, and callers receive errors that
// errs.KindOf understands.
package grpcclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/UsatovPavel/go-grpc-kit/grpcerr"
)

// Option configures [Dial].
type Option func(*config)

type config struct {
	insecure     bool
	callTimeout  time.Duration
	errorMapping bool
	logger       *slog.Logger
	unary        []grpc.UnaryClientInterceptor
	dialOptions  []grpc.DialOption
}

// WithInsecure disables transport security. Use it for local development
// and tests only. Without it, [Dial] uses TLS with the system root
// certificates.
func WithInsecure() Option {
	return func(c *config) { c.insecure = true }
}

// WithCallTimeout bounds every unary call to at most d. The timeout is
// always applied with context.WithTimeout, so the effective deadline is the
// earlier of the caller's deadline and now+d: a shorter caller deadline is
// kept, and a longer one (or none) is capped at d. Streaming calls are not
// affected. A value of zero or less disables the limit.
func WithCallTimeout(d time.Duration) Option {
	return func(c *config) { c.callTimeout = d }
}

// WithErrorMapping converts status errors returned by the server with
// grpcerr.FromStatus, for both unary and streaming calls, so callers can
// branch on errs.KindOf instead of status codes.
func WithErrorMapping() Option {
	return func(c *config) { c.errorMapping = true }
}

// WithLogger logs every unary call: successful calls at debug level and
// failed ones at warn level. A nil logger disables logging.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}

// WithInterceptors appends unary client interceptors. They run after the
// built-in ones, closest to the transport, in the order given.
func WithInterceptors(interceptors ...grpc.UnaryClientInterceptor) Option {
	return func(c *config) { c.unary = append(c.unary, interceptors...) }
}

// WithDialOptions passes additional options to grpc.NewClient, for example
// custom credentials, a context dialer or a service config. Credentials
// given here take precedence over the default TLS credentials.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(c *config) { c.dialOptions = append(c.dialOptions, opts...) }
}

// Dial creates a client connection to target using grpc.NewClient. As with
// grpc.NewClient, no I/O happens until the first call; the connection is
// established lazily. The caller must Close the returned connection.
func Dial(target string, opts ...Option) (*grpc.ClientConn, error) {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}

	var creds credentials.TransportCredentials
	if cfg.insecure {
		creds = insecure.NewCredentials()
	} else {
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}

	var unary []grpc.UnaryClientInterceptor
	var stream []grpc.StreamClientInterceptor
	if cfg.errorMapping {
		unary = append(unary, grpcerr.UnaryClientInterceptor())
		stream = append(stream, grpcerr.StreamClientInterceptor())
	}
	if cfg.logger != nil {
		unary = append(unary, loggingInterceptor(cfg.logger))
	}
	if cfg.callTimeout > 0 {
		unary = append(unary, TimeoutInterceptor(cfg.callTimeout))
	}
	unary = append(unary, cfg.unary...)

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithChainUnaryInterceptor(unary...),
		grpc.WithChainStreamInterceptor(stream...),
	}
	dialOpts = append(dialOpts, cfg.dialOptions...)

	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("grpcclient: dial %s: %w", target, err)
	}
	return conn, nil
}

// TimeoutInterceptor returns a unary client interceptor that limits each
// call to at most d. The resulting deadline is min(caller deadline, now+d).
// It is what
// [WithCallTimeout] installs; it is exported for use with custom
// connections. A d of zero or less makes it a no-op.
func TimeoutInterceptor(d time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if d > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func loggingInterceptor(l *slog.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		st := status.Convert(err)
		level := slog.LevelDebug
		if st.Code() != codes.OK {
			level = slog.LevelWarn
		}
		attrs := []slog.Attr{
			slog.String("grpc.method", method),
			slog.String("grpc.target", cc.Target()),
			slog.String("grpc.code", st.Code().String()),
			slog.Duration("grpc.duration", time.Since(start)),
		}
		if err != nil {
			attrs = append(attrs, slog.String("grpc.message", st.Message()))
		}
		l.LogAttrs(ctx, level, "grpc client call", attrs...)
		return err
	}
}
