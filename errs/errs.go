// Package errs defines transport-agnostic error kinds.
//
// Domain and application code classifies failures with a [Kind] instead of
// a transport-specific status code. Adapters at the edge (for example the
// grpcerr package) translate kinds into wire statuses and back, so the core
// of a service never imports gRPC or HTTP packages.
//
// The usual pattern is to declare sentinel errors once and wrap them:
//
//	var ErrUserNotFound = errs.New(errs.NotFound, "user not found")
//
//	func (r *Repo) Get(id string) (*User, error) {
//		...
//		return nil, fmt.Errorf("get user %q: %w", id, ErrUserNotFound)
//	}
//
// Callers can then match the exact sentinel with [errors.Is] or the broad
// category with [KindOf] / [IsKind].
package errs

import (
	"context"
	"errors"
	"strconv"
)

// Kind classifies an error by what the caller can do about it.
//
// The zero value is [Internal], so an unclassified error is treated as an
// unexpected server-side failure.
type Kind int

// Error kinds. They mirror the subset of gRPC codes that are meaningful for
// application logic, but carry no dependency on gRPC.
const (
	// Internal is an unexpected failure. Its details should not be exposed
	// to clients.
	Internal Kind = iota
	// NotFound means the requested entity does not exist.
	NotFound
	// AlreadyExists means the entity the caller tried to create exists.
	AlreadyExists
	// FailedPrecondition means the system is not in a state required for
	// the operation, and retrying without changing that state will not help.
	FailedPrecondition
	// InvalidArgument means the request itself is malformed or invalid.
	InvalidArgument
	// Unavailable means a dependency is temporarily unreachable; the call
	// may succeed if retried later.
	Unavailable
	// Unauthenticated means the caller did not provide valid credentials.
	Unauthenticated
	// PermissionDenied means the caller is known but not allowed to perform
	// the operation.
	PermissionDenied
	// ResourceExhausted means a quota or rate limit was hit.
	ResourceExhausted
	// DeadlineExceeded means the operation ran out of time.
	DeadlineExceeded
	// Canceled means the operation was canceled, usually by the caller.
	Canceled
)

var kindNames = [...]string{
	Internal:           "internal",
	NotFound:           "not found",
	AlreadyExists:      "already exists",
	FailedPrecondition: "failed precondition",
	InvalidArgument:    "invalid argument",
	Unavailable:        "unavailable",
	Unauthenticated:    "unauthenticated",
	PermissionDenied:   "permission denied",
	ResourceExhausted:  "resource exhausted",
	DeadlineExceeded:   "deadline exceeded",
	Canceled:           "canceled",
}

// String returns a human-readable name of the kind, such as "not found".
func (k Kind) String() string {
	if k >= 0 && int(k) < len(kindNames) {
		return kindNames[k]
	}
	return "kind(" + strconv.Itoa(int(k)) + ")"
}

// Error is an error annotated with a [Kind].
//
// Values are usually created once with [New] and used as sentinels, so
// [errors.Is] compares them by identity: two separately created errors with
// the same kind and message are not equal.
type Error struct {
	// Kind classifies the error.
	Kind Kind
	// Msg is a human-readable description. It may be shown to clients
	// unless Kind is Internal.
	Msg string
}

// New returns a new error of the given kind with the given message.
func New(kind Kind, msg string) *Error {
	return &Error{Kind: kind, Msg: msg}
}

// Error implements the error interface. It returns Msg, or the kind name if
// Msg is empty.
func (e *Error) Error() string {
	if e.Msg == "" {
		return e.Kind.String()
	}
	return e.Msg
}

// KindOf reports the kind of err.
//
// It returns the Kind of the first *Error found in err's chain. Otherwise
// [context.Canceled] maps to [Canceled], [context.DeadlineExceeded] maps to
// [DeadlineExceeded], and anything else (including nil) maps to [Internal].
// Callers should check err != nil first.
func KindOf(err error) Kind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	switch {
	case errors.Is(err, context.Canceled):
		return Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return DeadlineExceeded
	default:
		return Internal
	}
}

// IsKind reports whether err is non-nil and of the given kind.
func IsKind(err error, kind Kind) bool {
	return err != nil && KindOf(err) == kind
}
