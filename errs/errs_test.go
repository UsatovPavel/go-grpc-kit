package errs_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/UsatovPavel/go-grpc-kit/errs"
)

var errMissing = errs.New(errs.NotFound, "item missing")

func TestKindOf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want errs.Kind
	}{
		{"nil", nil, errs.Internal},
		{"plain error", errors.New("boom"), errs.Internal},
		{"sentinel", errMissing, errs.NotFound},
		{"wrapped sentinel", fmt.Errorf("load: %w", errMissing), errs.NotFound},
		{"double wrapped", fmt.Errorf("a: %w", fmt.Errorf("b: %w", errs.New(errs.Unavailable, "down"))), errs.Unavailable},
		{"context canceled", context.Canceled, errs.Canceled},
		{"wrapped deadline", fmt.Errorf("call: %w", context.DeadlineExceeded), errs.DeadlineExceeded},
		{"kind wins over context", fmt.Errorf("%w: %w", errs.New(errs.InvalidArgument, "bad"), context.Canceled), errs.InvalidArgument},
		{"joined", errors.Join(errors.New("x"), errs.New(errs.PermissionDenied, "no")), errs.PermissionDenied},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := errs.KindOf(tt.err); got != tt.want {
				t.Fatalf("KindOf() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsKind(t *testing.T) {
	t.Parallel()

	if errs.IsKind(nil, errs.Internal) {
		t.Error("IsKind(nil, Internal) = true, want false")
	}
	if !errs.IsKind(fmt.Errorf("x: %w", errMissing), errs.NotFound) {
		t.Error("IsKind(wrapped NotFound, NotFound) = false, want true")
	}
	if errs.IsKind(errMissing, errs.AlreadyExists) {
		t.Error("IsKind(NotFound, AlreadyExists) = true, want false")
	}
}

func TestIsComparesByIdentity(t *testing.T) {
	t.Parallel()

	twin := errs.New(errs.NotFound, "item missing")
	wrapped := fmt.Errorf("lookup: %w", errMissing)

	if !errors.Is(wrapped, errMissing) {
		t.Error("errors.Is(wrapped, sentinel) = false, want true")
	}
	if errors.Is(wrapped, twin) {
		t.Error("errors.Is(wrapped, twin) = true, want false: sentinels must compare by identity")
	}
}

func TestErrorMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  *errs.Error
		want string
	}{
		{"with message", errs.New(errs.NotFound, "no such thing"), "no such thing"},
		{"empty message falls back to kind", errs.New(errs.AlreadyExists, ""), "already exists"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.err.Error(); got != tt.want {
				t.Fatalf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestKindString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kind errs.Kind
		want string
	}{
		{errs.Internal, "internal"},
		{errs.FailedPrecondition, "failed precondition"},
		{errs.Canceled, "canceled"},
		{errs.Kind(99), "kind(99)"},
		{errs.Kind(-1), "kind(-1)"},
	}
	for _, tt := range tests {
		if got := tt.kind.String(); got != tt.want {
			t.Errorf("Kind(%d).String() = %q, want %q", int(tt.kind), got, tt.want)
		}
	}
}

func ExampleKindOf() {
	errNoSuchKey := errs.New(errs.NotFound, "no such key")
	err := fmt.Errorf("read config: %w", errNoSuchKey)

	fmt.Println(errs.KindOf(err))
	fmt.Println(errors.Is(err, errNoSuchKey))
	// Output:
	// not found
	// true
}
