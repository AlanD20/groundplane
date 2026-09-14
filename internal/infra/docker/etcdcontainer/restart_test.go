package etcdcontainer

import (
	"context"
	"errors"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// QA: REL-01. Rationale: closing the Controller's client must not stop its
// independent state store, even when closing the Docker client fails.
func TestClosePreservesEtcdRuntime(t *testing.T) {
	closeFailure := errors.New("Docker client close failed")
	for _, test := range []struct {
		name     string
		closeErr error
	}{
		{name: "normal shutdown"},
		{name: "close error remains visible", closeErr: closeFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine := &restartEngine{closeErr: test.closeErr}
			manager := Manager{client: engine}
			err := manager.Close()
			if !errors.Is(err, test.closeErr) || engine.closeCalls != 1 {
				t.Fatalf("Close = %v, client close calls = %d", err, engine.closeCalls)
			}
			if engine.inspectCalls != 0 || engine.stopCalls != 0 {
				t.Fatalf("Controller shutdown inspected/stopped etcd: %d/%d", engine.inspectCalls, engine.stopCalls)
			}
		})
	}
}

type restartEngine struct {
	engineClient
	closeErr     error
	closeCalls   int
	inspectCalls int
	stopCalls    int
}

func (engine *restartEngine) Close() error {
	engine.closeCalls++
	return engine.closeErr
}

func (engine *restartEngine) ContainerInspect(
	context.Context, string, client.ContainerInspectOptions,
) (client.ContainerInspectResult, error) {
	engine.inspectCalls++
	options := createOptions()
	return client.ContainerInspectResult{Container: container.InspectResponse{
		ID: "existing-etcd", Config: options.Config, HostConfig: options.HostConfig,
		State: &container.State{Running: true},
	}}, nil
}

func (engine *restartEngine) ContainerStop(
	context.Context, string, client.ContainerStopOptions,
) (client.ContainerStopResult, error) {
	engine.stopCalls++
	return client.ContainerStopResult{}, nil
}
