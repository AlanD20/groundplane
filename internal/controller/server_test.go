package controller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestServeReturnsNilAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server := &Server{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Mux:    http.NewServeMux(),
	}
	result := make(chan error, 1)
	go func() {
		result <- server.Serve(ctx, "127.0.0.1:0")
	}()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Serve() error = %v, want nil after cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() did not return after cancellation")
	}
}

func TestServeClassifiesListenFailureAsInternal(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	defer listener.Close()

	server := &Server{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Mux:    http.NewServeMux(),
	}
	err = server.Serve(context.Background(), listener.Addr().String())
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Serve() error = %v, want %q", err, errs.CodeInternal)
	}
}

func TestClassifyServeError(t *testing.T) {
	if err := classifyServeError(nil); err != nil {
		t.Fatalf("classifyServeError(nil) = %v, want nil", err)
	}
	if err := classifyServeError(http.ErrServerClosed); !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("classifyServeError(http.ErrServerClosed) = %v, want preserved sentinel", err)
	}

	cause := errors.New("accept failed")
	err := classifyServeError(cause)
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("classifyServeError(cause) = %v, want %q", err, errs.CodeInternal)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("classifyServeError(cause) = %v, want wrapped cause", err)
	}
}
