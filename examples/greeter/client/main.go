// Command client calls the example greeter service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/UsatovPavel/go-grpc-kit/errs"
	greeterv1 "github.com/UsatovPavel/go-grpc-kit/examples/greeter/greeterpb/greeter/v1"
	"github.com/UsatovPavel/go-grpc-kit/grpcclient"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "server address")
	name := flag.String("name", "World", "name to greet")
	lang := flag.String("lang", "", "two-letter language code")
	flag.Parse()

	if err := run(*addr, *name, *lang); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(addr, name, lang string) error {
	conn, err := grpcclient.Dial(addr,
		grpcclient.WithInsecure(),
		grpcclient.WithCallTimeout(2*time.Second),
		grpcclient.WithErrorMapping(),
		grpcclient.WithLogger(slog.New(slog.NewTextHandler(os.Stderr, nil))),
	)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	resp, err := greeterv1.NewGreeterServiceClient(conn).SayHello(context.Background(),
		&greeterv1.SayHelloRequest{Name: name, Language: lang})
	if err != nil {
		switch errs.KindOf(err) {
		case errs.NotFound:
			return fmt.Errorf("language not supported: %w", err)
		case errs.InvalidArgument:
			return fmt.Errorf("bad input: %w", err)
		default:
			return err
		}
	}
	fmt.Println(resp.GetMessage())
	return nil
}
