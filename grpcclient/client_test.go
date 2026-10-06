package grpcclient_test

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/UsatovPavel/go-grpc-kit/errs"
	"github.com/UsatovPavel/go-grpc-kit/grpcclient"
	"github.com/UsatovPavel/go-grpc-kit/grpcserver"
)

func TestTimeoutInterceptor(t *testing.T) {
	t.Parallel()

	const timeout = time.Second
	tests := []struct {
		name        string
		timeout     time.Duration
		ctxTimeout  time.Duration // 0 means no deadline on the incoming ctx
		wantDL      bool
		wantAtLeast time.Duration
		wantAtMost  time.Duration
	}{
		{"applies default when none", timeout, 0, true, timeout - 100*time.Millisecond, timeout},
		{"keeps shorter deadline", timeout, 100 * time.Millisecond, true, 0, 100 * time.Millisecond},
		{"keeps longer deadline", timeout, time.Hour, true, time.Hour - time.Minute, time.Hour},
		{"zero timeout is a no-op", 0, 0, false, 0, 0},
		{"negative timeout is a no-op", -time.Second, 0, false, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			if tt.ctxTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.ctxTimeout)
				defer cancel()
			}

			var gotDL bool
			var remaining time.Duration
			invoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				var dl time.Time
				dl, gotDL = ctx.Deadline()
				remaining = time.Until(dl)
				return nil
			}
			if err := grpcclient.TimeoutInterceptor(tt.timeout)(ctx, "/svc/M", nil, nil, nil, invoker); err != nil {
				t.Fatalf("interceptor returned %v", err)
			}
			if gotDL != tt.wantDL {
				t.Fatalf("has deadline = %v, want %v", gotDL, tt.wantDL)
			}
			if tt.wantDL && (remaining < tt.wantAtLeast || remaining > tt.wantAtMost) {
				t.Fatalf("remaining = %v, want in [%v, %v]", remaining, tt.wantAtLeast, tt.wantAtMost)
			}
		})
	}
}

// startHealthServer runs a grpcserver with only the health service over an
// in-memory listener and returns a dialer for it.
func startHealthServer(t *testing.T) grpc.DialOption {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpcserver.New(grpcserver.WithListener(lis), grpcserver.WithHealth())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})
}

func TestDialEndToEnd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mapping  bool
		service  string
		wantCode codes.Code
		wantKind errs.Kind
		wantErr  bool
	}{
		{"serving", true, "", codes.OK, 0, false},
		{"mapped not found", true, "missing.Service", codes.NotFound, errs.NotFound, true},
		{"raw not found", false, "missing.Service", codes.NotFound, errs.Internal, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var logs bytes.Buffer
			opts := []grpcclient.Option{
				grpcclient.WithInsecure(),
				grpcclient.WithCallTimeout(5 * time.Second),
				grpcclient.WithLogger(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))),
				grpcclient.WithDialOptions(startHealthServer(t)),
			}
			if tt.mapping {
				opts = append(opts, grpcclient.WithErrorMapping())
			}
			conn, err := grpcclient.Dial("passthrough:///bufnet", opts...)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer func() { _ = conn.Close() }()

			_, err = healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{Service: tt.service})
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("status code = %v, want %v", got, tt.wantCode)
			}
			if tt.wantErr {
				if got := errs.KindOf(err); got != tt.wantKind {
					t.Fatalf("errs.KindOf = %v, want %v", got, tt.wantKind)
				}
			}
			if want := "grpc.code=" + tt.wantCode.String(); !strings.Contains(logs.String(), want) {
				t.Fatalf("logs missing %q:\n%s", want, logs.String())
			}
		})
	}
}

func TestDialRequiresValidTarget(t *testing.T) {
	t.Parallel()

	if _, err := grpcclient.Dial("unknown-scheme://\x00bad"); err == nil {
		t.Fatal("Dial() with an invalid target succeeded")
	}
}
