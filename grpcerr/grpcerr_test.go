package grpcerr_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/UsatovPavel/go-grpc-kit/errs"
	"github.com/UsatovPavel/go-grpc-kit/grpcerr"
)

var errMissing = errs.New(errs.NotFound, "item missing")

func TestToStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantMsg  string
	}{
		{"nil", nil, codes.OK, ""},
		{"sentinel", errMissing, codes.NotFound, "item missing"},
		{"wrapped keeps context", fmt.Errorf("load: %w", errMissing), codes.NotFound, "load: item missing"},
		{"already exists", errs.New(errs.AlreadyExists, "dup"), codes.AlreadyExists, "dup"},
		{"failed precondition", errs.New(errs.FailedPrecondition, "not ready"), codes.FailedPrecondition, "not ready"},
		{"invalid argument", errs.New(errs.InvalidArgument, "bad"), codes.InvalidArgument, "bad"},
		{"unavailable", errs.New(errs.Unavailable, "down"), codes.Unavailable, "down"},
		{"unauthenticated", errs.New(errs.Unauthenticated, "who"), codes.Unauthenticated, "who"},
		{"permission denied", errs.New(errs.PermissionDenied, "no"), codes.PermissionDenied, "no"},
		{"resource exhausted", errs.New(errs.ResourceExhausted, "slow down"), codes.ResourceExhausted, "slow down"},
		{"internal hides message", errs.New(errs.Internal, "db password is hunter2"), codes.Internal, grpcerr.InternalMessage},
		{"plain error hides message", errors.New("stack trace here"), codes.Internal, grpcerr.InternalMessage},
		{"context canceled", context.Canceled, codes.Canceled, context.Canceled.Error()},
		{"wrapped deadline", fmt.Errorf("query: %w", context.DeadlineExceeded), codes.DeadlineExceeded, "query: context deadline exceeded"},
		{"status passes through", status.Error(codes.Aborted, "conflict"), codes.Aborted, "conflict"},
		{"wrapped status passes through", fmt.Errorf("x: %w", status.Error(codes.Unimplemented, "nope")), codes.Unimplemented, "nope"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := grpcerr.ToStatus(tt.err)
			if tt.err == nil {
				if got != nil {
					t.Fatalf("ToStatus(nil) = %v, want nil", got)
				}
				return
			}
			st, ok := status.FromError(got)
			if !ok {
				t.Fatalf("ToStatus() = %v, not a status error", got)
			}
			if st.Code() != tt.wantCode || st.Message() != tt.wantMsg {
				t.Fatalf("ToStatus() = (%v, %q), want (%v, %q)", st.Code(), st.Message(), tt.wantCode, tt.wantMsg)
			}
		})
	}
}

func TestFromStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		wantKind errs.Kind
		wantMsg  string
	}{
		{"not found", status.Error(codes.NotFound, "gone"), errs.NotFound, "gone"},
		{"invalid argument", status.Error(codes.InvalidArgument, "bad"), errs.InvalidArgument, "bad"},
		{"unavailable", status.Error(codes.Unavailable, "down"), errs.Unavailable, "down"},
		{"deadline", status.Error(codes.DeadlineExceeded, "slow"), errs.DeadlineExceeded, "slow"},
		{"canceled", status.Error(codes.Canceled, "bye"), errs.Canceled, "bye"},
		{"unmapped code is internal", status.Error(codes.DataLoss, "oops"), errs.Internal, "oops"},
		{"unknown code is internal", status.Error(codes.Unknown, "?"), errs.Internal, "?"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := grpcerr.FromStatus(tt.err)

			var e *errs.Error
			if !errors.As(got, &e) {
				t.Fatalf("FromStatus() = %T, does not unwrap to *errs.Error", got)
			}
			if e.Kind != tt.wantKind || got.Error() != tt.wantMsg {
				t.Fatalf("FromStatus() = (%v, %q), want (%v, %q)", e.Kind, got.Error(), tt.wantKind, tt.wantMsg)
			}
			if errs.KindOf(got) != tt.wantKind {
				t.Fatalf("errs.KindOf() = %v, want %v", errs.KindOf(got), tt.wantKind)
			}
			if status.Code(got) != status.Code(tt.err) {
				t.Fatalf("status.Code() = %v, want original %v", status.Code(got), status.Code(tt.err))
			}
		})
	}
}

func TestFromStatusPassThrough(t *testing.T) {
	t.Parallel()

	plain := errors.New("plain")
	tests := []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"io.EOF", io.EOF},
		{"plain", plain},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := grpcerr.FromStatus(tt.err); !errors.Is(got, tt.err) || (tt.err == nil && got != nil) {
				t.Fatalf("FromStatus(%v) = %v, want unchanged", tt.err, got)
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	for kind := errs.NotFound; kind <= errs.Canceled; kind++ {
		t.Run(kind.String(), func(t *testing.T) {
			t.Parallel()
			back := grpcerr.FromStatus(grpcerr.ToStatus(errs.New(kind, "msg")))
			if got := errs.KindOf(back); got != kind {
				t.Fatalf("round trip kind = %v, want %v", got, kind)
			}
			// A converted error passes through ToStatus again unchanged,
			// so proxies preserve the upstream code.
			if got := status.Code(grpcerr.ToStatus(back)); got != grpcerr.CodeOf(kind) {
				t.Fatalf("re-encoded code = %v, want %v", got, grpcerr.CodeOf(kind))
			}
		})
	}
}

func TestUnaryServerInterceptor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{"ok", nil, codes.OK},
		{"kind", errMissing, codes.NotFound},
		{"plain", errors.New("boom"), codes.Internal},
	}
	interceptor := grpcerr.UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resp, err := interceptor(context.Background(), "req", info, func(context.Context, any) (any, error) {
				return "resp", tt.err
			})
			if resp != "resp" {
				t.Errorf("resp = %v, want handler response", resp)
			}
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("code = %v, want %v", got, tt.wantCode)
			}
		})
	}
}

func TestStreamServerInterceptor(t *testing.T) {
	t.Parallel()

	interceptor := grpcerr.StreamServerInterceptor()
	info := &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}
	err := interceptor(nil, nil, info, func(any, grpc.ServerStream) error {
		return fmt.Errorf("recv: %w", errs.New(errs.PermissionDenied, "denied"))
	})
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("code = %v, want %v", got, codes.PermissionDenied)
	}
}

func TestUnaryClientInterceptor(t *testing.T) {
	t.Parallel()

	interceptor := grpcerr.UnaryClientInterceptor()
	err := interceptor(context.Background(), "/test.Service/Method", nil, nil, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			return status.Error(codes.AlreadyExists, "dup")
		})
	if !errs.IsKind(err, errs.AlreadyExists) {
		t.Fatalf("err = %v (kind %v), want AlreadyExists", err, errs.KindOf(err))
	}
}
