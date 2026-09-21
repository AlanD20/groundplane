package handlers

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"
)

// Rationale: native upgrade qualification must not race an unbound HTTP
// listener, including when an earlier configured address successfully bound.
func TestHTTPStartupReadinessRequiresEveryListener(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(map[bool]string{false: "all-ready", true: "partial-bind-failure"}[blocked], func(t *testing.T) {
			ready := make(chan struct{})
			server := &Server{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Mux: http.NewServeMux(),
				onHTTPReady: func() { close(ready) }}
			addresses := []string{"127.0.0.1:0", "127.0.0.1:0"}
			if blocked {
				listener, err := net.Listen("tcp", addresses[1])
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				addresses[1] = listener.Addr().String()
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- server.Serve(ctx, addresses) }()
			if blocked {
				if err := <-done; err == nil {
					t.Fatal("partial bind succeeded")
				}
				select {
				case <-ready:
					t.Fatal("partial bind qualified HTTP readiness")
				default:
				}
				return
			}
			select {
			case <-ready:
			case err := <-done:
				t.Fatalf("server exited before readiness: %v", err)
			case <-time.After(time.Second):
				t.Fatal("listeners did not signal readiness")
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
