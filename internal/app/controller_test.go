package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestNewControllerRejectsInvalidHTTPListenerBeforeEtcdConstruction(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "controller.yaml")
	contents := []byte(
		"environment_pool: 10.0.0.0/9\nsystem_pool: 10.128.0.0/9\n" +
			"runner:\n  network_pool: 10.240.0.0/24\n  host_uid_range: 200000-200007\n" +
			"  subuid_range: 300000-824287\n  subgid_range: 900000-1424287\n" +
			"etcd:\n  endpoints: [\"://invalid\"]\nlisten:\n  http: 0.0.0.0:8080\nlog:\n  file:\n    enabled: false\n",
	)
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
	agentFailure := errors.New("agent channel failed")
	closeFailure := errors.New("close failed")
	containerFailure := errors.New("container close failed")
	tests := []struct {
		name          string
		serveErr      error
		agentErr      error
		closeErr      error
		containerErr  error
		wantServe     bool
		wantAgent     bool
		wantClose     bool
		wantContainer bool
		wantFailure   bool
	}{
		{name: "normal cancellation"},
		{name: "serve failure", serveErr: serveFailure, wantServe: true, wantFailure: true},
		{name: "Agent channel failure", agentErr: agentFailure, wantAgent: true, wantFailure: true},
		{name: "close failure", closeErr: closeFailure, wantClose: true, wantFailure: true},
		{name: "Docker close failure", containerErr: containerFailure, wantContainer: true, wantFailure: true},
		{
			name:        "joined serve and close failures",
			serveErr:    serveFailure,
			closeErr:    closeFailure,
			wantServe:   true,
			wantClose:   true,
			wantFailure: true,
		},
		{
			name:        "joined Agent and close failures",
			agentErr:    agentFailure,
			closeErr:    closeFailure,
			wantAgent:   true,
			wantClose:   true,
			wantFailure: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server := &fakeControllerServer{err: test.serveErr}
			agent := &fakeControllerAgentChannel{err: test.agentErr, stopped: make(chan struct{})}
			scheduler := &fakeControllerScheduler{stopped: make(chan struct{})}
			controllerTasks := &fakeControllerScheduler{stopped: make(chan struct{})}
			localAgent := &fakeControllerScheduler{stopped: make(chan struct{})}
			container := &fakeOwnedContainer{
				err: test.containerErr, localAgentStopped: localAgent.stopped,
			}
			store := &fakeOwnedStore{
				err:                    test.closeErr,
				serverReturned:         &server.returned,
				schedulerStopped:       scheduler.stopped,
				controllerTasksStopped: controllerTasks.stopped,
				agentStopped:           agent.stopped,
				containerClosed:        &container.closed,
			}
			controller := &Controller{
				Config:          config.DefaultControllerConfig(),
				server:          server,
				agent:           agent,
				scheduler:       scheduler,
				controllerTasks: controllerTasks,
				localAgent:      localAgent,
				container:       container,
				store:           store,
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			runDone := make(chan error, 1)
			go func() { runDone <- controller.Run(ctx) }()
			var err error
			select {
			case err = <-runDone:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for Controller shutdown")
			}

			if store.closeCalls != 1 {
				t.Fatalf("Store.Close calls = %d, want 1", store.closeCalls)
			}
			if container.closeCalls != 1 {
				t.Fatalf("Agent container Close calls = %d, want 1", container.closeCalls)
			}
			if container.closedBeforeLocalAgentStopped {
				t.Fatal("Agent container Close ran before local Agent reconciliation stopped")
			}
			if store.closedBeforeContainer {
				t.Fatal("Store.Close ran before the Agent container client closed")
			}
			if store.closedBeforeServerReturned {
				t.Fatal("Store.Close ran before Server.Serve returned")
			}
			if store.closedBeforeSchedulerStopped {
				t.Fatal("Store.Close ran before the scheduler stopped")
			}
			if store.closedBeforeControllerTasksStopped {
				t.Fatal("Store.Close ran before the Controller Task runner stopped")
			}
			if store.closedBeforeAgentStopped {
				t.Fatal("Store.Close ran before the Agent channel stopped")
			}
			if (err != nil) != test.wantFailure {
				t.Fatalf("Controller.Run error = %v, want failure = %t", err, test.wantFailure)
			}
			if test.wantFailure && !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("Controller.Run error = %v, want canonical %q error", err, errs.CodeInternal)
			}
			if errors.Is(err, serveFailure) != test.wantServe {
				t.Fatalf(
					"Controller.Run preserves serve failure = %t, want %t",
					errors.Is(err, serveFailure),
					test.wantServe,
				)
			}
			if errors.Is(err, agentFailure) != test.wantAgent {
				t.Fatalf(
					"Controller.Run preserves Agent failure = %t, want %t",
					errors.Is(err, agentFailure),
					test.wantAgent,
				)
			}
			if errors.Is(err, closeFailure) != test.wantClose {
				t.Fatalf(
					"Controller.Run preserves close failure = %t, want %t",
					errors.Is(err, closeFailure),
					test.wantClose,
				)
			}
			if errors.Is(err, containerFailure) != test.wantContainer {
				t.Fatalf("Controller.Run error = %v, want Docker close failure", err)
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

type fakeControllerAgentChannel struct {
	err     error
	stopped chan struct{}
}

func (channel *fakeControllerAgentChannel) Run(ctx context.Context) error {
	defer close(channel.stopped)
	if channel.err != nil {
		return channel.err
	}
	<-ctx.Done()
	return nil
}

func (s *fakeControllerScheduler) Run(ctx context.Context) {
	<-ctx.Done()
	close(s.stopped)
}

type fakeOwnedStore struct {
	err                                error
	closeCalls                         int
	serverReturned                     *bool
	schedulerStopped                   <-chan struct{}
	controllerTasksStopped             <-chan struct{}
	agentStopped                       <-chan struct{}
	containerClosed                    *bool
	closedBeforeServerReturned         bool
	closedBeforeSchedulerStopped       bool
	closedBeforeControllerTasksStopped bool
	closedBeforeAgentStopped           bool
	closedBeforeContainer              bool
}

func (s *fakeOwnedStore) Close() error {
	s.closeCalls++
	s.closedBeforeServerReturned = !*s.serverReturned
	select {
	case <-s.schedulerStopped:
	default:
		s.closedBeforeSchedulerStopped = true
	}
	select {
	case <-s.controllerTasksStopped:
	default:
		s.closedBeforeControllerTasksStopped = true
	}
	select {
	case <-s.agentStopped:
	default:
		s.closedBeforeAgentStopped = true
	}
	s.closedBeforeContainer = !*s.containerClosed
	return s.err
}

type fakeOwnedContainer struct {
	err                           error
	closeCalls                    int
	closed                        bool
	localAgentStopped             <-chan struct{}
	closedBeforeLocalAgentStopped bool
}

func (container *fakeOwnedContainer) Close() error {
	container.closeCalls++
	select {
	case <-container.localAgentStopped:
	default:
		container.closedBeforeLocalAgentStopped = true
	}
	container.closed = true
	return container.err
}
