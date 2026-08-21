package agentchannel

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	testAgentID      = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testOtherAgentID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

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

// Rationale: enrollment must not miss the first Ready when the Agent reports it
// before the Controller installs its readiness subscription.
func TestRegistryReadyClosesImmediatelyAfterMatchingReady(t *testing.T) {
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	defer session.Close()
	if err := session.RecordReady(testTime(), 3); err != nil {
		t.Fatalf("record Ready: %v", err)
	}
	ready, err := registry.Ready(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("subscribe Ready: %v", err)
	}
	select {
	case <-ready:
	default:
		t.Fatal("matching Ready subscription remained open")
	}
}

// Rationale: a Ready report belongs to one live authenticated stream; after
// disconnect, provisioning must wait for a fresh same-generation stream and
// report instead of reusing historical in-memory evidence.
func TestRegistryReadyWaitsForFreshReportAfterMatchingSessionCloses(t *testing.T) {
	registry := NewRegistry()
	first, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open first session: %v", err)
	}
	if err := first.RecordReady(testTime(), 3); err != nil {
		t.Fatalf("record first Ready: %v", err)
	}
	first.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready, err := registry.Ready(ctx, testAgentID, 7)
	if err != nil {
		t.Fatalf("subscribe after disconnect: %v", err)
	}
	select {
	case <-ready:
		t.Fatal("historical Ready satisfied a subscription after disconnect")
	default:
	}

	replacement, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open replacement session: %v", err)
	}
	defer replacement.Close()
	if err := replacement.RecordReady(testTime().Add(time.Second), 2); err != nil {
		t.Fatalf("record replacement Ready: %v", err)
	}
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("fresh matching Ready did not satisfy subscription")
	}
}

// Rationale: enrollment may subscribe before the Agent opens its stream, so
// subscription registration and the later matching Ready must be atomic.
func TestRegistryReadyClosesWhenMatchingReadyArrivesAfterSubscribe(t *testing.T) {
	registry := NewRegistry()
	ready, err := registry.Ready(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("subscribe Ready: %v", err)
	}
	select {
	case <-ready:
		t.Fatal("Ready subscription closed before a matching report")
	default:
	}

	session, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	defer session.Close()
	if err := session.RecordReady(testTime(), 2); err != nil {
		t.Fatalf("record Ready: %v", err)
	}
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("Ready subscription did not close")
	}
}

// Rationale: canceling enrollment must release its pending readiness
// subscription without changing a later subscriber for the same generation.
func TestRegistryReadyCancellationReleasesSubscription(t *testing.T) {
	registry := NewRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	ready, err := registry.Ready(ctx, testAgentID, 7)
	if err != nil {
		t.Fatalf("subscribe Ready: %v", err)
	}
	cancel()
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("canceled Ready subscription did not close")
	}

	later, err := registry.Ready(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("subscribe later Ready: %v", err)
	}
	select {
	case <-later:
		t.Fatal("canceled subscription closed a later subscription")
	default:
	}
	session, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	defer session.Close()
	if err := session.RecordReady(testTime(), 1); err != nil {
		t.Fatalf("record Ready: %v", err)
	}
	select {
	case <-later:
	case <-time.After(time.Second):
		t.Fatal("later Ready subscription did not close")
	}
}

// Rationale: a Ready report from a replacement generation must never satisfy
// enrollment or health waiting for an older durable generation.
func TestRegistryReadyIgnoresDifferentGeneration(t *testing.T) {
	registry := NewRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready, err := registry.Ready(ctx, testAgentID, 7)
	if err != nil {
		t.Fatalf("subscribe Ready: %v", err)
	}
	session, err := registry.Open(context.Background(), testAgentID, 8)
	if err != nil {
		t.Fatalf("open replacement: %v", err)
	}
	defer session.Close()
	if err := session.RecordReady(testTime(), 2); err != nil {
		t.Fatalf("record replacement Ready: %v", err)
	}
	select {
	case <-ready:
		t.Fatal("replacement generation satisfied stale Ready subscription")
	default:
	}
}

// Rationale: stale lifecycle work must never stop, revoke, or wait on a newer
// replacement generation that reused the same Agent id.
func TestRegistryLifecycleOperationsFenceGenerationMismatch(t *testing.T) {
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), testAgentID, 8)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	defer session.Close()

	assertStateConflict(t, registry.StopAssignments(context.Background(), testAgentID, 7))
	assertStateConflict(t, registry.Revoke(context.Background(), testAgentID, 7))
	assertStateConflict(t, registry.WaitOffline(context.Background(), testAgentID, 7))
	if !session.AssignmentsAllowed() {
		t.Fatal("stale lifecycle operation fenced the replacement")
	}
	select {
	case <-session.Done():
		t.Fatal("stale lifecycle operation canceled the replacement")
	default:
	}
}

// Rationale: deletion retries are allowed after disconnect or after the
// in-memory session disappeared, and each matching operation must be a no-op.
func TestRegistryLifecycleOperationsAreIdempotentWhenAbsentOrOffline(t *testing.T) {
	registry := NewRegistry()
	for _, operation := range []struct {
		name string
		run  func() error
	}{
		{name: "stop absent", run: func() error {
			return registry.StopAssignments(context.Background(), testAgentID, 7)
		}},
		{name: "revoke absent", run: func() error {
			return registry.Revoke(context.Background(), testAgentID, 7)
		}},
		{name: "wait absent", run: func() error {
			return registry.WaitOffline(context.Background(), testAgentID, 7)
		}},
	} {
		if err := operation.run(); err != nil {
			t.Fatalf("%s: %v", operation.name, err)
		}
	}
	_, err := registry.Open(context.Background(), testAgentID, 7)
	assertStateConflict(t, err)

	session, err := registry.Open(context.Background(), testOtherAgentID, 7)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	session.Close()
	if err := registry.StopAssignments(context.Background(), testOtherAgentID, 7); err != nil {
		t.Fatalf("stop offline: %v", err)
	}
	if err := registry.Revoke(context.Background(), testOtherAgentID, 7); err != nil {
		t.Fatalf("revoke offline: %v", err)
	}
	if err := registry.WaitOffline(context.Background(), testOtherAgentID, 7); err != nil {
		t.Fatalf("wait offline: %v", err)
	}
}

// Rationale: quiescence belongs to an Agent generation, not one connection;
// replacing a live same-generation stream must not silently reopen assignment.
func TestRegistrySameGenerationReconnectRemainsStoppedAfterLiveQuiescence(t *testing.T) {
	registry := NewRegistry()
	first, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open first session: %v", err)
	}
	if err := registry.StopAssignments(context.Background(), testAgentID, 7); err != nil {
		t.Fatalf("stop assignments: %v", err)
	}
	replacement, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open replacement: %v", err)
	}
	defer replacement.Close()
	select {
	case <-first.Done():
	default:
		t.Fatal("same-generation replacement did not fence the old stream")
	}
	if replacement.AssignmentsAllowed() {
		t.Fatal("same-generation replacement reopened assignments")
	}
	first.Close()
}

// Rationale: quiescence recorded before connection or after disconnect must
// still fence a later same-generation stream during resumable deletion.
func TestRegistrySameGenerationReconnectRemainsStoppedWhenAbsentOrOffline(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*testing.T, *Registry)
	}{
		{
			name: "absent",
			prepare: func(t *testing.T, registry *Registry) {
				t.Helper()
				if err := registry.StopAssignments(context.Background(), testAgentID, 7); err != nil {
					t.Fatalf("stop absent assignments: %v", err)
				}
			},
		},
		{
			name: "offline",
			prepare: func(t *testing.T, registry *Registry) {
				t.Helper()
				session, err := registry.Open(context.Background(), testAgentID, 7)
				if err != nil {
					t.Fatalf("open initial session: %v", err)
				}
				session.Close()
				if err := registry.StopAssignments(context.Background(), testAgentID, 7); err != nil {
					t.Fatalf("stop offline assignments: %v", err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := NewRegistry()
			test.prepare(t, registry)
			session, err := registry.Open(context.Background(), testAgentID, 7)
			if err != nil {
				t.Fatalf("open same-generation session: %v", err)
			}
			defer session.Close()
			if session.AssignmentsAllowed() {
				t.Fatal("same-generation session reopened assignments")
			}
		})
	}
}

// Rationale: removal can revoke while authentication is paused before Open;
// the later Open must observe durable-policy fencing despite no live session.
func TestRegistryRevokeWhileAbsentRejectsPausedSameGenerationOpen(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Revoke(context.Background(), testAgentID, 7); err != nil {
		t.Fatalf("revoke absent generation: %v", err)
	}
	_, err := registry.Open(context.Background(), testAgentID, 6)
	assertStateConflict(t, err)
	_, err = registry.Open(context.Background(), testAgentID, 7)
	assertStateConflict(t, err)
	replacement, err := registry.Open(context.Background(), testAgentID, 8)
	if err != nil {
		t.Fatalf("open newer generation: %v", err)
	}
	if !replacement.AssignmentsAllowed() {
		t.Fatal("newer unfenced generation inherited stale lifecycle policy")
	}
	replacement.Close()
}

// Rationale: invalid Open inputs must fail before a newer attempt can cancel
// or otherwise mutate the current authenticated session.
func TestRegistryOpenRejectsInvalidInputsWithoutFencingCurrentSession(t *testing.T) {
	registry := NewRegistry()
	current, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open current session: %v", err)
	}
	defer current.Close()

	if _, err := registry.Open(nil, testAgentID, 8); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("nil-context Open() error = %v, want internal", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.Open(canceled, testAgentID, 8); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled-context Open() error = %v, want context.Canceled", err)
	}
	if _, err := registry.Open(context.Background(), testAgentID, 0); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("zero-generation Open() error = %v, want validation.failed", err)
	}
	select {
	case <-current.Done():
		t.Fatal("invalid Open input fenced the current session")
	default:
	}
	if err := current.RecordReady(testTime(), 1); err != nil {
		t.Fatalf("current session lost authority: %v", err)
	}
}

// Rationale: a parent canceled while Open waits for the registry lock is
// already canceled at the mutation boundary and must not fence the live session.
func TestRegistryOpenRejectsParentCanceledWhileWaitingForMutation(t *testing.T) {
	registry := NewRegistry()
	current, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open current session: %v", err)
	}
	defer current.Close()

	parent, cancel := context.WithCancel(context.Background())
	checked := make(chan struct{})
	observed := &observedErrorContext{Context: parent, checked: checked}
	registry.mu.Lock()
	result := make(chan error, 1)
	go func() {
		_, openErr := registry.Open(observed, testAgentID, 8)
		result <- openErr
	}()
	<-checked
	cancel()
	registry.mu.Unlock()

	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Open() error = %v, want context.Canceled", err)
	}
	select {
	case <-current.Done():
		t.Fatal("canceled Open fenced the current session")
	default:
	}
}

// Rationale: revoking one generation must fence its stream idempotently while
// still allowing a separately authenticated newer generation to replace it.
func TestRegistryRevokeFencesOnlyMatchingGeneration(t *testing.T) {
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	if err := registry.StopAssignments(context.Background(), testAgentID, 7); err != nil {
		t.Fatalf("stop assignments: %v", err)
	}
	if err := registry.Revoke(context.Background(), testAgentID, 7); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if err := registry.Revoke(context.Background(), testAgentID, 7); err != nil {
		t.Fatalf("repeat revoke: %v", err)
	}
	select {
	case <-session.Done():
	default:
		t.Fatal("matching revocation did not cancel the session")
	}
	session.Close()

	replacement, err := registry.Open(context.Background(), testAgentID, 8)
	if err != nil {
		t.Fatalf("open replacement: %v", err)
	}
	replacement.Close()
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

type observedErrorContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

func (ctx *observedErrorContext) Err() error {
	err := ctx.Context.Err()
	ctx.once.Do(func() { close(ctx.checked) })
	return err
}
