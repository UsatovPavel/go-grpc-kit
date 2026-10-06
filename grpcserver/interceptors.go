package grpcserver

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/UsatovPavel/go-grpc-kit/grpcerr"
)

// levelFor picks a log level for a finished call: server-side failures are
// errors, client-side failures are warnings.
func levelFor(code codes.Code) slog.Level {
	switch code {
	case codes.OK:
		return slog.LevelInfo
	case codes.Internal, codes.Unknown, codes.DataLoss, codes.Unimplemented, codes.Unavailable:
		return slog.LevelError
	default:
		return slog.LevelWarn
	}
}

func logCall(ctx context.Context, l *slog.Logger, method string, start time.Time, err error) {
	st := status.Convert(err)
	attrs := []slog.Attr{
		slog.String("grpc.method", method),
		slog.String("grpc.code", st.Code().String()),
		slog.Duration("grpc.duration", time.Since(start)),
	}
	if err != nil {
		attrs = append(attrs, slog.String("grpc.message", st.Message()))
	}
	l.LogAttrs(ctx, levelFor(st.Code()), "grpc call", attrs...)
}

func unaryLogging(l *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logCall(ctx, l, info.FullMethod, start, err)
		return resp, err
	}
}

func streamLogging(l *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		logCall(ss.Context(), l, info.FullMethod, start, err)
		return err
	}
}

func recovered(ctx context.Context, l *slog.Logger, method string, p any) error {
	l.LogAttrs(ctx, slog.LevelError, "panic recovered",
		slog.String("grpc.method", method),
		slog.Any("panic", p),
		slog.String("stack", string(debug.Stack())),
	)
	return status.Error(codes.Internal, grpcerr.InternalMessage)
}

func unaryRecovery(l *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if p := recover(); p != nil {
				resp, err = nil, recovered(ctx, l, info.FullMethod, p)
			}
		}()
		return handler(ctx, req)
	}
}

func streamRecovery(l *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if p := recover(); p != nil {
				err = recovered(ss.Context(), l, info.FullMethod, p)
			}
		}()
		return handler(srv, ss)
	}
}

func validate(v protovalidate.Validator, req any) error {
	msg, ok := req.(proto.Message)
	if !ok {
		return nil
	}
	err := v.Validate(msg)
	if err == nil {
		return nil
	}
	var verr *protovalidate.ValidationError
	if errors.As(err, &verr) {
		return status.Error(codes.InvalidArgument, verr.Error())
	}
	// Compilation or runtime errors mean the rules themselves are broken,
	// which is a server bug rather than a bad request.
	return status.Error(codes.Internal, grpcerr.InternalMessage)
}

func unaryValidation(v protovalidate.Validator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := validate(v, req); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func streamValidation(v protovalidate.Validator) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &validatingStream{ServerStream: ss, v: v})
	}
}

// validatingStream validates every message received from the client.
type validatingStream struct {
	grpc.ServerStream
	v protovalidate.Validator
}

func (s *validatingStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	return validate(s.v, m)
}
