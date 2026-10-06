// Package grpcerr translates between errs kinds and gRPC statuses.
//
// On the server side, [UnaryServerInterceptor] and [StreamServerInterceptor]
// convert errors returned by handlers into gRPC status errors using
// [ToStatus]. On the client side, [FromStatus] turns a status error back
// into an error whose kind can be inspected with errs.KindOf.
package grpcerr

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/UsatovPavel/go-grpc-kit/errs"
)

// InternalMessage is the status message sent to clients in place of the
// real message of an Internal (or unclassified) error.
const InternalMessage = "internal error"

var kindToCode = map[errs.Kind]codes.Code{
	errs.Internal:           codes.Internal,
	errs.NotFound:           codes.NotFound,
	errs.AlreadyExists:      codes.AlreadyExists,
	errs.FailedPrecondition: codes.FailedPrecondition,
	errs.InvalidArgument:    codes.InvalidArgument,
	errs.Unavailable:        codes.Unavailable,
	errs.Unauthenticated:    codes.Unauthenticated,
	errs.PermissionDenied:   codes.PermissionDenied,
	errs.ResourceExhausted:  codes.ResourceExhausted,
	errs.DeadlineExceeded:   codes.DeadlineExceeded,
	errs.Canceled:           codes.Canceled,
}

var codeToKind = func() map[codes.Code]errs.Kind {
	m := make(map[codes.Code]errs.Kind, len(kindToCode))
	for k, c := range kindToCode {
		m[c] = k
	}
	return m
}()

// CodeOf returns the gRPC code that corresponds to kind. Unknown kinds map
// to codes.Internal.
func CodeOf(kind errs.Kind) codes.Code {
	if c, ok := kindToCode[kind]; ok {
		return c
	}
	return codes.Internal
}

// KindOfCode returns the kind that corresponds to a gRPC code. Codes with no
// direct counterpart (Unknown, Aborted, OutOfRange, Unimplemented, DataLoss)
// map to errs.Internal.
func KindOfCode(code codes.Code) errs.Kind {
	if k, ok := codeToKind[code]; ok {
		return k
	}
	return errs.Internal
}

type grpcStatuser interface {
	GRPCStatus() *status.Status
}

// ToStatus converts err into a gRPC status error.
//
// The rules, applied in order:
//   - nil stays nil;
//   - an error that already carries a gRPC status (anywhere in its chain) is
//     returned as that status unchanged;
//   - context.Canceled and context.DeadlineExceeded become codes.Canceled and
//     codes.DeadlineExceeded;
//   - an error whose kind is not Internal becomes the matching code, with
//     err.Error() as the message;
//   - everything else becomes codes.Internal with [InternalMessage], so that
//     implementation details do not leak to clients.
func ToStatus(err error) error {
	if err == nil {
		return nil
	}
	var se grpcStatuser
	if errors.As(err, &se) {
		if st := se.GRPCStatus(); st != nil {
			return st.Err()
		}
	}
	switch kind := errs.KindOf(err); kind {
	case errs.Internal:
		return status.Error(codes.Internal, InternalMessage)
	default:
		return status.Error(CodeOf(kind), err.Error())
	}
}

// FromStatus converts a gRPC status error received by a client into an error
// carrying the matching errs.Kind.
//
// The returned error unwraps to an *errs.Error, so errs.KindOf and
// errors.As work on it, and it still implements GRPCStatus, so
// status.FromError and status.Code keep returning the original status.
// Errors that carry no status (including io.EOF) are returned unchanged, and
// nil stays nil.
func FromStatus(err error) error {
	if err == nil {
		return nil
	}
	var se grpcStatuser
	if !errors.As(err, &se) {
		return err
	}
	st := se.GRPCStatus()
	if st == nil || st.Code() == codes.OK {
		return err
	}
	return &remoteError{
		err: errs.New(KindOfCode(st.Code()), st.Message()),
		st:  st,
	}
}

// remoteError is a status error received from a peer, re-expressed as an
// *errs.Error while keeping the original status available.
type remoteError struct {
	err *errs.Error
	st  *status.Status
}

func (e *remoteError) Error() string { return e.err.Error() }

func (e *remoteError) Unwrap() error { return e.err }

func (e *remoteError) GRPCStatus() *status.Status { return e.st }

// UnaryServerInterceptor returns a server interceptor that converts handler
// errors with [ToStatus].
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		return resp, ToStatus(err)
	}
}

// StreamServerInterceptor returns a stream server interceptor that converts
// handler errors with [ToStatus].
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return ToStatus(handler(srv, ss))
	}
}

// UnaryClientInterceptor returns a client interceptor that converts errors
// returned by the server with [FromStatus].
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return FromStatus(invoker(ctx, method, req, reply, cc, opts...))
	}
}

// StreamClientInterceptor returns a client interceptor that converts errors
// returned while opening a stream, and by its SendMsg, RecvMsg and Header
// methods, with [FromStatus].
func StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, FromStatus(err)
		}
		return &clientStream{ClientStream: cs}, nil
	}
}

type clientStream struct {
	grpc.ClientStream
}

func (s *clientStream) SendMsg(m any) error { return FromStatus(s.ClientStream.SendMsg(m)) }

func (s *clientStream) RecvMsg(m any) error { return FromStatus(s.ClientStream.RecvMsg(m)) }

func (s *clientStream) Header() (metadata.MD, error) {
	md, err := s.ClientStream.Header()
	return md, FromStatus(err)
}
