package grpcserver_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/UsatovPavel/go-grpc-kit/errs"
	"github.com/UsatovPavel/go-grpc-kit/grpcserver"
)

// probe is a hand-written test service whose behaviour is selected by the
// request value, so the tests need no generated code.
type probe struct {
	started chan struct{}
}

func (p *probe) handle(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	select {
	case p.started <- struct{}{}:
	default:
	}
	switch in.GetValue() {
	case "panic":
		panic("boom")
	case "notfound":
		return nil, errs.New(errs.NotFound, "no such thing")
	case "plain":
		return nil, errors.New("secret detail")
	case "status":
		return nil, status.Error(codes.Aborted, "conflict")
	case "slow":
		time.Sleep(200 * time.Millisecond)
	case "hang":
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return wrapperspb.String("ok:" + in.GetValue()), nil
}

const (
	callMethod   = "/kit.test.Probe/Call"
	streamMethod = "/kit.test.Probe/Stream"
)

var probeDesc = grpc.ServiceDesc{
	ServiceName: "kit.test.Probe",
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Call",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			in := new(wrapperspb.StringValue)
			if err := dec(in); err != nil {
				return nil, err
			}
			h := func(ctx context.Context, req any) (any, error) {
				return srv.(*probe).handle(ctx, req.(*wrapperspb.StringValue))
			}
			return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: callMethod}, h)
		},
	}},
	Streams: []grpc.StreamDesc{{
		StreamName:    "Stream",
		ServerStreams: true,
		ClientStreams: true,
		Handler: func(srv any, stream grpc.ServerStream) error {
			in := new(wrapperspb.StringValue)
			if err := stream.RecvMsg(in); err != nil {
				return err
			}
			out, err := srv.(*probe).handle(stream.Context(), in)
			if err != nil {
				return err
			}
			return stream.SendMsg(out)
		},
	}},
}

type harness struct {
	conn   *grpc.ClientConn
	probe  *probe
	stop   context.CancelFunc
	runErr chan error
}

func start(t *testing.T, opts ...grpcserver.Option) *harness {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	p := &probe{started: make(chan struct{}, 1)}
	srv := grpcserver.New(append([]grpcserver.Option{grpcserver.WithListener(lis)}, opts...)...)
	srv.Register(func(s *grpc.Server) { s.RegisterService(&probeDesc, p) })

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	h := &harness{conn: conn, probe: p, stop: cancel, runErr: runErr}
	t.Cleanup(func() {
		_ = conn.Close()
		cancel()
		<-runErr
	})
	return h
}

func (h *harness) call(ctx context.Context, value string) (*wrapperspb.StringValue, error) {
	out := new(wrapperspb.StringValue)
	err := h.conn.Invoke(ctx, callMethod, wrapperspb.String(value), out)
	return out, err
}

func (h *harness) stream(ctx context.Context, value string) error {
	cs, err := h.conn.NewStream(ctx, &probeDesc.Streams[0], streamMethod)
	if err != nil {
		return err
	}
	if err := cs.SendMsg(wrapperspb.String(value)); err != nil {
		return err
	}
	if err := cs.CloseSend(); err != nil {
		return err
	}
	return cs.RecvMsg(new(wrapperspb.StringValue))
}

func TestUnaryChain(t *testing.T) {
	t.Parallel()
	h := start(t)

	tests := []struct {
		value    string
		wantCode codes.Code
		wantMsg  string
	}{
		{"hello", codes.OK, ""},
		{"panic", codes.Internal, "internal error"},
		{"notfound", codes.NotFound, "no such thing"},
		{"plain", codes.Internal, "internal error"},
		{"status", codes.Aborted, "conflict"},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			t.Parallel()
			out, err := h.call(context.Background(), tt.value)
			st := status.Convert(err)
			if st.Code() != tt.wantCode || st.Message() != tt.wantMsg {
				t.Fatalf("got (%v, %q), want (%v, %q)", st.Code(), st.Message(), tt.wantCode, tt.wantMsg)
			}
			if err == nil && out.GetValue() != "ok:"+tt.value {
				t.Fatalf("response = %q", out.GetValue())
			}
		})
	}
}

func TestStreamChain(t *testing.T) {
	t.Parallel()
	h := start(t)

	tests := []struct {
		value    string
		wantCode codes.Code
	}{
		{"hello", codes.OK},
		{"panic", codes.Internal},
		{"notfound", codes.NotFound},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			t.Parallel()
			if got := status.Code(h.stream(context.Background(), tt.value)); got != tt.wantCode {
				t.Fatalf("code = %v, want %v", got, tt.wantCode)
			}
		})
	}
}

type fakeValidator struct{ err error }

func (f fakeValidator) Validate(proto.Message, ...protovalidate.ValidationOption) error { return f.err }

func TestValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{"valid", nil, codes.OK},
		{"violation", &protovalidate.ValidationError{}, codes.InvalidArgument},
		{"broken rules", errors.New("compilation error"), codes.Internal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := start(t, grpcserver.WithValidator(fakeValidator{err: tt.err}))
			_, err := h.call(context.Background(), "hello")
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("unary code = %v, want %v", got, tt.wantCode)
			}
			if got := status.Code(h.stream(context.Background(), "hello")); got != tt.wantCode {
				t.Fatalf("stream code = %v, want %v", got, tt.wantCode)
			}
		})
	}
}

func TestUserInterceptorErrorsAreMapped(t *testing.T) {
	t.Parallel()

	deny := func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
		return nil, errs.New(errs.Unauthenticated, "missing token")
	}
	h := start(t, grpcserver.WithInterceptors(deny))

	_, err := h.call(context.Background(), "hello")
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("code = %v, want %v", got, codes.Unauthenticated)
	}
}

// syncBuffer is a bytes.Buffer safe for concurrent writes by the logger.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestLoggingSeesMappedCode(t *testing.T) {
	t.Parallel()

	var buf syncBuffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	h := start(t, grpcserver.WithLogger(logger))

	if _, err := h.call(context.Background(), "notfound"); err == nil {
		t.Fatal("expected an error")
	}
	logs := buf.String()
	for _, want := range []string{"grpc.method=" + callMethod, "grpc.code=NotFound", "level=WARN"} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs missing %q:\n%s", want, logs)
		}
	}
}

func TestGracefulShutdownWaitsForInFlightCalls(t *testing.T) {
	t.Parallel()
	h := start(t, grpcserver.WithShutdownTimeout(5*time.Second))

	callErr := make(chan error, 1)
	go func() {
		_, err := h.call(context.Background(), "slow")
		callErr <- err
	}()
	<-h.probe.started
	h.stop()

	if err := <-callErr; err != nil {
		t.Fatalf("in-flight call failed during graceful shutdown: %v", err)
	}
	if err := <-h.runErr; err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}
	h.runErr <- nil // let the cleanup drain the channel again
}

func TestShutdownTimeoutForcesStop(t *testing.T) {
	t.Parallel()
	h := start(t, grpcserver.WithShutdownTimeout(50*time.Millisecond))

	callErr := make(chan error, 1)
	go func() {
		_, err := h.call(context.Background(), "hang")
		callErr <- err
	}()
	<-h.probe.started
	begin := time.Now()
	h.stop()

	select {
	case err := <-h.runErr:
		if err != nil {
			t.Fatalf("Run() = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the shutdown timeout")
	}
	h.runErr <- nil
	if elapsed := time.Since(begin); elapsed < 50*time.Millisecond {
		t.Errorf("Run returned after %v, before the shutdown timeout", elapsed)
	}
	if code := status.Code(<-callErr); code != codes.Unavailable && code != codes.Canceled {
		t.Fatalf("hung call code = %v, want Unavailable or Canceled", code)
	}
}

func TestHealth(t *testing.T) {
	t.Parallel()
	h := start(t, grpcserver.WithHealth())

	resp, err := healthpb.NewHealthClient(h.conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status = %v, want SERVING", resp.GetStatus())
	}
}

func TestHealthAccessor(t *testing.T) {
	t.Parallel()

	if grpcserver.New().Health() != nil {
		t.Error("Health() without WithHealth should be nil")
	}
	if grpcserver.New(grpcserver.WithHealth()).Health() == nil {
		t.Error("Health() with WithHealth should not be nil")
	}
}

func TestRunListenError(t *testing.T) {
	t.Parallel()

	err := grpcserver.New(grpcserver.WithAddress("not-an-address")).Run(context.Background())
	if err == nil {
		t.Fatal("Run() = nil, want listen error")
	}
}
