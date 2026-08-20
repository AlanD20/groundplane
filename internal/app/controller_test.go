package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestNewControllerRejectsInvalidHTTPListenerBeforeEtcdConstruction(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "controller.yaml")
	contents := []byte("etcd:\n  endpoints: [\"://invalid\"]\nlisten:\n  http: 0.0.0.0:8080\nlog:\n  file:\n    enabled: false\n")
	if err := os.WriteFile(configPath, contents, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	controller, err := NewController(context.Background(), configPath)
	if controller != nil {
		t.Cleanup(func() { _ = controller.store.Close() })
	}
	if err == nil {
		t.Fatal("NewController() error = nil, want invalid human HTTP listener error")
	}
	if !strings.Contains(err.Error(), "controller listen.http") {
		t.Fatalf("NewController() error = %v, want human HTTP listener validation before etcd construction", err)
	}
}

// Rationale: the Controller owns the etcd client and must release it on every
// exit path without losing either a serving failure or a close failure.
func TestControllerRunClosesOwnedStoreAndPreservesErrors(t *testing.T) {
	t.Parallel()

	serveFailure := errors.New("listen failed")
	closeFailure := errors.New("close failed")
	tests := []struct {
		name        string
		serveErr    error
		closeErr    error
		wantServe   bool
		wantClose   bool
		wantFailure bool
	}{
		{name: "normal cancellation"},
		{name: "serve failure", serveErr: serveFailure, wantServe: true, wantFailure: true},
		{name: "close failure", closeErr: closeFailure, wantClose: true, wantFailure: true},
		{
			name:        "joined serve and close failures",
			serveErr:    serveFailure,
			closeErr:    closeFailure,
			wantServe:   true,
			wantClose:   true,
			wantFailure: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server := &fakeControllerServer{err: test.serveErr}
			scheduler := &fakeControllerScheduler{stopped: make(chan struct{})}
			store := &fakeOwnedStore{
				err:              test.closeErr,
				serverReturned:   &server.returned,
				schedulerStopped: scheduler.stopped,
			}
			controller := &Controller{
				Config:    config.DefaultControllerConfig(),
				server:    server,
				scheduler: scheduler,
				store:     store,
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := controller.Run(ctx)

			if store.closeCalls != 1 {
				t.Fatalf("Store.Close calls = %d, want 1", store.closeCalls)
			}
			if store.closedBeforeServerReturned {
				t.Fatal("Store.Close ran before Server.Serve returned")
			}
			if store.closedBeforeSchedulerStopped {
				t.Fatal("Store.Close ran before the scheduler stopped")
			}
			if (err != nil) != test.wantFailure {
				t.Fatalf("Controller.Run error = %v, want failure = %t", err, test.wantFailure)
			}
			if test.wantFailure && !errors.Is(err, errs.New(errs.CodeInternal, "")) {
				t.Fatalf("Controller.Run error = %v, want canonical %q error", err, errs.CodeInternal)
			}
			if errors.Is(err, serveFailure) != test.wantServe {
				t.Fatalf("Controller.Run preserves serve failure = %t, want %t", errors.Is(err, serveFailure), test.wantServe)
			}
			if errors.Is(err, closeFailure) != test.wantClose {
				t.Fatalf("Controller.Run preserves close failure = %t, want %t", errors.Is(err, closeFailure), test.wantClose)
			}
		})
	}
}

type fakeControllerServer struct {
	err      error
	returned bool
}

func (s *fakeControllerServer) Serve(context.Context, string) error {
	s.returned = true
	return s.err
}

type fakeControllerScheduler struct {
	stopped chan struct{}
}

func (s *fakeControllerScheduler) Run(ctx context.Context) {
	<-ctx.Done()
	close(s.stopped)
}

type fakeOwnedStore struct {
	err                          error
	closeCalls                   int
	serverReturned               *bool
	schedulerStopped             <-chan struct{}
	closedBeforeServerReturned   bool
	closedBeforeSchedulerStopped bool
}

func (s *fakeOwnedStore) Close() error {
	s.closeCalls++
	s.closedBeforeServerReturned = !*s.serverReturned
	select {
	case <-s.schedulerStopped:
	default:
		s.closedBeforeSchedulerStopped = true
	}
	return s.err
}
