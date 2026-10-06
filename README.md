# go-grpc-kit

[![CI](https://github.com/UsatovPavel/go-grpc-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/UsatovPavel/go-grpc-kit/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/UsatovPavel/go-grpc-kit.svg)](https://pkg.go.dev/github.com/UsatovPavel/go-grpc-kit)

Small building blocks for gRPC services in Go: server bootstrap with
graceful shutdown, client dialing with sane defaults, and error kinds
that keep gRPC out of your domain code.

## Why

Every gRPC service ends up with the same setup code: a listener, an
interceptor chain, panic recovery, request validation, health checks,
signal handling, a shutdown that doesn't drop in-flight calls, and some
way to turn "not found" in the business layer into `codes.NotFound` on
the wire. This module packages that setup as plain functions and options
over `google.golang.org/grpc`. It doesn't add a framework, code
generation or a registry. You still work with `*grpc.Server` and
`*grpc.ClientConn` directly.

## Packages

| Package      | What it does |
|--------------|--------------|
| `errs`       | Transport-agnostic error kinds (`NotFound`, `InvalidArgument`, ...). No gRPC import. |
| `grpcerr`    | Maps kinds to gRPC statuses and back; server and client interceptors. |
| `grpcserver` | Server builder: logging, recovery, error mapping, protovalidate, health, reflection, graceful `Run(ctx)`. |
| `grpcclient` | `Dial` on top of `grpc.NewClient`: TLS by default, default call timeout, error mapping, logging. |

## Install

```sh
go get github.com/UsatovPavel/go-grpc-kit
```

Requires Go 1.25 or newer.

## Quick start

Domain code returns kinds:

```go
var ErrUserNotFound = errs.New(errs.NotFound, "user not found")

func (s *Users) Get(ctx context.Context, id string) (*User, error) {
	u, ok := s.byID[id]
	if !ok {
		return nil, fmt.Errorf("get user %q: %w", id, ErrUserNotFound)
	}
	return u, nil
}
```

The server turns them into statuses:

```go
srv := grpcserver.New(
	grpcserver.WithAddress(":50051"),
	grpcserver.WithLogger(slog.Default()),
	grpcserver.WithValidation(),   // protovalidate rules from your .proto files
	grpcserver.WithHealth(),
	grpcserver.WithReflection(),
	grpcserver.WithShutdownTimeout(10*time.Second),
)
srv.Register(func(s *grpc.Server) {
	pb.RegisterUsersServiceServer(s, handler)
})

ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
if err := srv.Run(ctx); err != nil {
	log.Fatal(err)
}
```

The client turns statuses back into kinds:

```go
conn, err := grpcclient.Dial("users:50051",
	grpcclient.WithInsecure(),
	grpcclient.WithCallTimeout(2*time.Second),
	grpcclient.WithErrorMapping(),
)
if err != nil {
	return err
}
defer conn.Close()

_, err = pb.NewUsersServiceClient(conn).GetUser(ctx, req)
if errs.IsKind(err, errs.NotFound) {
	// handle missing user
}
```

A complete runnable example lives in [`examples/greeter`](examples/greeter):

```sh
go run ./examples/greeter/server
go run ./examples/greeter/client -name Alice -lang es
```

## Design notes

### Kinds instead of statuses in domain code

Business logic should describe *what went wrong*, not *how to say it over
a particular protocol*. If a repository returns `status.Error(codes.NotFound, ...)`,
then the domain package depends on gRPC, the same code can't sit behind
an HTTP handler or a queue consumer without re-mapping, and tests have to
reason about wire codes.

`errs.Kind` is a small, closed set of categories that every transport can
express. Sentinels created with `errs.New` compare by identity, so
`errors.Is(err, ErrUserNotFound)` matches only that exact error. Use
`errs.KindOf` to branch on the broader category. Translation happens once,
at the edge, in `grpcerr`:

- an error that already carries a gRPC status passes through unchanged;
- `context.Canceled` / `context.DeadlineExceeded` become `Canceled` / `DeadlineExceeded`;
- a non-internal kind becomes the matching code, with `err.Error()` as the message;
- `Internal` and unclassified errors become `codes.Internal` with a fixed
  `"internal error"` message, so stack traces, SQL and hostnames don't leak to clients.

On the client, `grpcerr.FromStatus` restores the kind and keeps the
original status reachable via `status.FromError`. A service that forwards
an upstream error therefore keeps its code.

### Interceptor order

Server chain, outermost first:

```
logging → panic recovery → error mapping → user interceptors → validation → handler
```

- **Logging** is outermost, so it records the final code and the total
  latency, including panics converted to `Internal`.
- **Recovery** sits above everything that runs user code, so a panic in a
  handler, a validator or your own interceptor becomes `codes.Internal`
  and doesn't crash the process.
- **Error mapping** sits *above* user interceptors and validation. An auth
  interceptor can return `errs.New(errs.Unauthenticated, ...)` and still
  produce the right code. If mapping were innermost, only handler errors
  would be translated.
- **User interceptors** run before validation, so authentication can
  reject a request before any work is spent on it.
- **Validation** is closest to the handler and rejects bad input with
  `codes.InvalidArgument`.

Client chain, outermost first:

```
error mapping → logging → call timeout → user interceptors → transport
```

The client logger sees raw statuses, and the default timeout applies only
when the caller's context has no deadline.

### Graceful shutdown

`Run(ctx)` blocks until `ctx` is cancelled. It then marks the health
service `NOT_SERVING`, calls `GracefulStop` so in-flight calls can finish,
and falls back to `Stop` once the shutdown timeout passes. A hung handler
can't hold the process forever.

## Development

```sh
go test -race ./...
golangci-lint run ./...
# regenerate example code (requires buf, protoc-gen-go, protoc-gen-go-grpc)
cd examples/greeter && buf generate
```

## License

[MIT](LICENSE)
