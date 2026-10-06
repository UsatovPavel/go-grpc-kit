// Package greeter is a small example service built with go-grpc-kit.
//
// The service implementation only deals with errs kinds; the kit turns
// them into gRPC statuses on the way out. Run the server and client with:
//
//	go run ./examples/greeter/server
//	go run ./examples/greeter/client -name Alice -lang es
package greeter

import (
	"context"
	"fmt"

	"github.com/UsatovPavel/go-grpc-kit/errs"
	greeterv1 "github.com/UsatovPavel/go-grpc-kit/examples/greeter/greeterpb/greeter/v1"
)

// ErrUnsupportedLanguage is returned for languages without a greeting.
var ErrUnsupportedLanguage = errs.New(errs.NotFound, "no greeting for this language")

var greetings = map[string]string{
	"en": "Hello",
	"es": "Hola",
	"fr": "Bonjour",
	"de": "Hallo",
}

// Service implements greeterv1.GreeterServiceServer.
type Service struct {
	greeterv1.UnimplementedGreeterServiceServer
}

// SayHello greets the requested name. Input shape (non-empty name, two
// letter language code) is enforced by protovalidate rules in the proto
// file, so the handler only checks business rules.
func (Service) SayHello(_ context.Context, req *greeterv1.SayHelloRequest) (*greeterv1.SayHelloResponse, error) {
	lang := req.GetLanguage()
	if lang == "" {
		lang = "en"
	}
	word, ok := greetings[lang]
	if !ok {
		return nil, fmt.Errorf("language %q: %w", lang, ErrUnsupportedLanguage)
	}
	return &greeterv1.SayHelloResponse{Message: word + ", " + req.GetName() + "!"}, nil
}
