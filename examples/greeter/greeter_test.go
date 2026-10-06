package greeter_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/UsatovPavel/go-grpc-kit/errs"
	"github.com/UsatovPavel/go-grpc-kit/examples/greeter"
	greeterv1 "github.com/UsatovPavel/go-grpc-kit/examples/greeter/greeterpb/greeter/v1"
	"github.com/UsatovPavel/go-grpc-kit/grpcclient"
	"github.com/UsatovPavel/go-grpc-kit/grpcserver"
)

func newClient(t *testing.T) greeterv1.GreeterServiceClient {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpcserver.New(grpcserver.WithListener(lis), grpcserver.WithValidation())
	srv.Register(func(s *grpc.Server) { greeterv1.RegisterGreeterServiceServer(s, greeter.Service{}) })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	conn, err := grpcclient.Dial("passthrough:///bufnet",
		grpcclient.WithInsecure(),
		grpcclient.WithErrorMapping(),
		grpcclient.WithDialOptions(grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		})),
	)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		cancel()
		<-done
	})
	return greeterv1.NewGreeterServiceClient(conn)
}

func TestSayHello(t *testing.T) {
	t.Parallel()
	client := newClient(t)

	tests := []struct {
		name     string
		req      *greeterv1.SayHelloRequest
		want     string
		wantKind errs.Kind
		wantErr  bool
	}{
		{"default language", &greeterv1.SayHelloRequest{Name: "Alice"}, "Hello, Alice!", 0, false},
		{"spanish", &greeterv1.SayHelloRequest{Name: "Bob", Language: "es"}, "Hola, Bob!", 0, false},
		{"unsupported language", &greeterv1.SayHelloRequest{Name: "Eve", Language: "xx"}, "", errs.NotFound, true},
		{"empty name fails validation", &greeterv1.SayHelloRequest{}, "", errs.InvalidArgument, true},
		{"bad language code fails validation", &greeterv1.SayHelloRequest{Name: "Al", Language: "ENG"}, "", errs.InvalidArgument, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resp, err := client.SayHello(context.Background(), tt.req)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if got := errs.KindOf(err); got != tt.wantKind {
					t.Fatalf("kind = %v, want %v (err: %v)", got, tt.wantKind, err)
				}
				return
			}
			if resp.GetMessage() != tt.want {
				t.Fatalf("message = %q, want %q", resp.GetMessage(), tt.want)
			}
		})
	}
}
