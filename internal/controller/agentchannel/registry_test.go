package agentchannel

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const testAgentID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"

// Rationale: a replaced connection must lose authority without being able to
// mark its replacement offline or mutate its readiness state.
func TestRegistryFencesReplacementSessions(t *testing.T) {
	registry := NewRegistry()
	first, err := registry.Open(context.Background(), testAgentID, 1)
	if err != nil {
		t.Fatalf("open first session: %v", err)
	}
	second, err := registry.Open(context.Background(), testAgentID, 2)
	if err != nil {
		t.Fatalf("open replacement session: %v", err)
	}

	select {
	case <-first.Done():
	default:
		t.Fatal("replaced session was not canceled")
	}
	assertStateConflict(t, first.RecordReady(testTime(), 3))
	first.Close()
	snapshot, ok := registry.Snapshot(testAgentID)
	if !ok || !snapshot.Online || snapshot.Generation != 2 {
		t.Fatalf("replacement snapshot = %+v, found = %v", snapshot, ok)
	}
	_, err = registry.Open(context.Background(), testAgentID, 1)
	assertStateConflict(t, err)
	second.Close()
}

// Rationale: Agent removal must quiesce assignment, invoke durable credential
// revocation for the active generation, fence the stream, and expose offline wait.
func TestRegistryRemovalHooksAndOfflineWait(t *testing.T) {
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	if err := registry.StopAssignments(testAgentID); err != nil {
		t.Fatalf("stop assignments: %v", err)
	}
	if session.AssignmentsAllowed() {
		t.Fatal("assignments remained enabled")
	}

	var revokedID string
	var revokedGeneration uint64
	err = registry.Revoke(context.Background(), testAgentID, func(_ context.Context, id string, generation uint64) error {
		revokedID = id
		revokedGeneration = generation
		return nil
	})
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if revokedID != testAgentID || revokedGeneration != 7 {
		t.Fatalf("revocation target = %q generation %d", revokedID, revokedGeneration)
	}
	select {
	case <-session.Done():
	default:
		t.Fatal("revoked session was not canceled")
	}

	session.Close()
	if err := registry.WaitOffline(context.Background(), testAgentID); err != nil {
		t.Fatalf("wait offline: %v", err)
	}
	snapshot, _ := registry.Snapshot(testAgentID)
	if snapshot.Online || !snapshot.Revoked || !snapshot.AssignmentsStopped {
		t.Fatalf("removal snapshot = %+v", snapshot)
	}
	_, err = registry.Open(context.Background(), testAgentID, 8)
	assertStateConflict(t, err)
}

func TestRegistryRevocationFailureRollsBackRevokingState(t *testing.T) {
	// Rationale: a failed durable hook must not claim the credential is revoked
	// or fence the live connection, while assignment quiescence remains safe.
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), testAgentID, 3)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	wantErr := errors.New("durable revocation failed")
	err = registry.Revoke(context.Background(), testAgentID, func(context.Context, string, uint64) error {
		return wantErr
	})
	if err == nil {
		t.Fatal("Revoke succeeded after hook failure")
	}
	snapshot, ok := registry.Snapshot(testAgentID)
	if !ok || snapshot.Revoked || !snapshot.Online || !snapshot.AssignmentsStopped {
		t.Fatalf("snapshot after failed revocation = %+v, found = %v", snapshot, ok)
	}
	select {
	case <-session.Done():
		t.Fatal("failed durable revocation canceled the session")
	default:
	}

	replacement, err := registry.Open(context.Background(), testAgentID, 4)
	if err != nil {
		t.Fatalf("open after revocation rollback: %v", err)
	}
	session.Close()
	replacement.Close()
}

func TestRegistryRejectsOpenWhileRevocationIsInProgress(t *testing.T) {
	// Rationale: reconnecting during the unlocked durable hook would otherwise
	// let a new session escape the generation being revoked.
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), testAgentID, 8)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	hookEntered := make(chan struct{})
	releaseHook := make(chan struct{})
	revokeDone := make(chan error, 1)
	go func() {
		revokeDone <- registry.Revoke(context.Background(), testAgentID, func(context.Context, string, uint64) error {
			close(hookEntered)
			<-releaseHook
			return nil
		})
	}()
	<-hookEntered

	_, err = registry.Open(context.Background(), testAgentID, 9)
	assertStateConflict(t, err)
	assertStateConflict(t, registry.Revoke(context.Background(), testAgentID, func(context.Context, string, uint64) error {
		return nil
	}))
	close(releaseHook)
	if err := <-revokeDone; err != nil {
		t.Fatalf("revoke: %v", err)
	}
	select {
	case <-session.Done():
	default:
		t.Fatal("successful revocation did not cancel the session")
	}
	session.Close()
}

func assertStateConflict(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("expected state conflict, got %v", err)
	}
	var domainError *errs.Error
	if !errors.As(err, &domainError) {
		t.Fatalf("expected domain error, got %T", err)
	}
	if got := domainError.HTTPStatus(); got != 409 {
		t.Fatalf("expected status 409, got %d", got)
	}
}
