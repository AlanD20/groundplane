package handlers

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

// QA: REL-01; already-cancelled local multi-listener Serve, not live restart.
// Rationale: Pre-cancelled startup must clean up and return without reporting a false failure.
func TestServeReturnsNilAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server := &Server{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Mux:    http.NewServeMux(),
	}
	result := make(chan error, 1)
	go func() {
		result <- server.Serve(ctx, []string{"127.0.0.1:0", "127.0.0.1:0"})
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

// QA: HOST-02, UI-05; local occupied-port failure, not host bootstrap.
// Rationale: A listen conflict must surface as the canonical internal failure instead of successful startup.
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
	err = server.Serve(context.Background(), []string{listener.Addr().String()})
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Serve() error = %v, want %q", err, errs.CodeInternal)
	}
}

// QA: HOST-02, UI-05; local error/cause handling, not actual accept failure.
// Rationale: Preserve normal close and the causal error of unexpected serving failures.
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
