package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/docker/agentcontainer"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the application boundary must preserve authenticated channel
// generation and readiness values without exposing controller package types.
func TestLocalAgentSessionsAdapterTranslatesSnapshotAndReady(t *testing.T) {
	t.Parallel()

	registry := agentchannel.NewRegistry()
	adapter, err := NewSessions(registry)
	if err != nil {
		t.Fatalf("NewSessions() error = %v", err)
	}
	session, err := registry.Open(context.Background(), runtimeAdapterAgentID, 7)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer session.Close()
	wantReady := time.Date(2026, time.August, 20, 15, 0, 0, 0, time.UTC)
	if err := session.RecordReady(wantReady, 4, "v0.4.2"); err != nil {
		t.Fatalf("RecordReady() error = %v", err)
	}

	ready, err := adapter.Ready(context.Background(), runtimeAdapterAgentID, 7)
	if err != nil {
		t.Fatalf("Ready() error = %v", err)
	}
	select {
	case <-ready:
	default:
		t.Fatal("translated Ready subscription remained open")
	}
	want := localagent.SessionSnapshot{
		Generation: 7,
		Online:     true,
		LastReady:  wantReady,
		Capacity:   4,
		Version:    "v0.4.2",
	}
	if got, ok := adapter.Snapshot(runtimeAdapterAgentID); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() = %#v, %t; want %#v, true", got, ok, want)
	}
}

// Rationale: the application adapter must preserve generation-conflict
// classification so stale lifecycle work remains distinguishable from outages.
func TestLocalAgentSessionsAdapterPreservesGenerationConflict(t *testing.T) {
	t.Parallel()

	registry := agentchannel.NewRegistry()
	adapter, err := NewSessions(registry)
	if err != nil {
		t.Fatalf("NewSessions() error = %v", err)
	}
	session, err := registry.Open(context.Background(), runtimeAdapterAgentID, 8)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer session.Close()
	if err := adapter.Revoke(context.Background(), runtimeAdapterAgentID, 7); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("Revoke() error = %v, want state.conflict", err)
	}
}

// Rationale: localagent's numeric generation must become one canonical Docker
// label value for both convergence and guarded removal.
func TestLocalAgentContainerAdapterTranslatesDesiredIdentity(t *testing.T) {
	t.Parallel()

	lifecycle := &fakeAgentContainerLifecycle{}
	adapter, err := NewContainer(lifecycle)
	if err != nil {
		t.Fatalf("NewContainer() error = %v", err)
	}
	want := localagent.ContainerDesired{
		AgentID:    runtimeAdapterAgentID,
		Image:      testAppAgentImage,
		Generation: 7,
	}
	if err := adapter.Converge(context.Background(), want); err != nil {
		t.Fatalf("Converge() error = %v", err)
	}
	if lifecycle.desired.AgentID != want.AgentID || lifecycle.desired.Image != want.Image ||
		lifecycle.desired.Generation != "7" {
		t.Fatalf("translated desired = %#v", lifecycle.desired)
	}
	if err := adapter.Remove(context.Background(), want.AgentID, want.Generation); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if lifecycle.removeAgentID != want.AgentID || lifecycle.removeGeneration != "7" {
		t.Fatalf("translated removal = %q generation %q", lifecycle.removeAgentID, lifecycle.removeGeneration)
	}
}

// Rationale: the application adapter must preserve Docker ownership mismatch
// classification for the lifecycle manager's sanitized port boundary.
func TestLocalAgentContainerAdapterPreservesOwnershipConflict(t *testing.T) {
	t.Parallel()

	lifecycle := &fakeAgentContainerLifecycle{removeErr: errs.New(errs.KindStateConflict, "replacement")}
	adapter, err := NewContainer(lifecycle)
	if err != nil {
		t.Fatalf("NewContainer() error = %v", err)
	}
	err = adapter.Remove(context.Background(), runtimeAdapterAgentID, 7)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Remove() error = %v, want state.conflict", err)
	}
}

const testAppAgentImage = "ghcr.io/aland20/groundplane-agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type fakeAgentContainerLifecycle struct {
	desired          agentcontainer.Desired
	removeAgentID    string
	removeGeneration string
	removeErr        error
}

func (lifecycle *fakeAgentContainerLifecycle) Reconcile(
	_ context.Context,
	desired agentcontainer.Desired,
) (agentcontainer.Result, error) {
	lifecycle.desired = desired
	return agentcontainer.Result{}, nil
}

func (lifecycle *fakeAgentContainerLifecycle) Remove(
	_ context.Context,
	agentID string,
	generation string,
) error {
	lifecycle.removeAgentID = agentID
	lifecycle.removeGeneration = generation
	return lifecycle.removeErr
}
