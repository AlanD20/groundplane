package agentchannel

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Rationale: update preparation must be reversible without resetting the
// monotonic lifecycle fence or requiring an Agent reconnection to make progress.
func TestRegistryAssignmentPauseResumesAndWakesDispatch(t *testing.T) {
	registry := NewRegistry()
	session := openAdmissionSession(t, registry, 7)
	resume, err := registry.PauseAssignments(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer resume()
	assertAssignmentSendRejected(t, session)
	if session.AssignmentsAllowed() {
		t.Fatal("paused session allows assignments")
	}
	snapshot, _ := registry.Snapshot(testAgentID)
	if !snapshot.AssignmentsStopped || !snapshot.Online {
		t.Fatalf("paused snapshot = %+v", snapshot)
	}
	resume()
	resume()
	assertAdmissionAllowed(t, session)
	select {
	case <-session.taskDispatchWake():
	default:
		t.Fatal("resume did not wake the live dispatcher")
	}
}

// Rationale: one operation can release only its own pause; an equal-generation
// reconnect must inherit every pause that is still held.
func TestRegistryAssignmentPauseOwnsOnlyItsAdmissionHold(t *testing.T) {
	registry := NewRegistry()
	openAdmissionSession(t, registry, 7)
	first, err := registry.PauseAssignments(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer first()
	second, err := registry.PauseAssignments(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer second()
	reconnected := openAdmissionSession(t, registry, 7)
	first()
	if reconnected.AssignmentsAllowed() {
		t.Fatal("one released pause bypassed the other operation")
	}
	second()
	assertAdmissionAllowed(t, reconnected)
}

// Rationale: resuming update preparation must never reopen a deletion fence,
// revocation, or an unrelated newer session generation.
func TestRegistryAssignmentPausePreservesLifecycleAuthority(t *testing.T) {
	for _, mode := range []string{"stop", "revoke", "newer-generation"} {
		t.Run(mode, func(t *testing.T) {
			registry := NewRegistry()
			session := openAdmissionSession(t, registry, 7)
			resume, err := registry.PauseAssignments(context.Background(), testAgentID, 7)
			if err != nil {
				t.Fatal(err)
			}
			defer resume()
			switch mode {
			case "stop":
				err = registry.StopAssignments(context.Background(), testAgentID, 7)
			case "revoke":
				err = registry.Revoke(context.Background(), testAgentID, 7)
			case "newer-generation":
				newer := openAdmissionSession(t, registry, 8)
				assertAdmissionAllowed(t, newer)
				resume()
				assertAdmissionAllowed(t, newer)
			}
			if err != nil {
				t.Fatal(err)
			}
			resume()
			assertAssignmentSendRejected(t, session)
		})
	}
}

// Rationale: cancellation while a prior send drains must undo only the temporary
// pause and must not terminate the already admitted assignment payload.
func TestRegistryAssignmentPauseCancellationRestoresAdmission(t *testing.T) {
	registry := NewRegistry()
	session := openAdmissionSession(t, registry, 7)
	releaseSend, sent := startUncooperativeAssignmentSend(t, session)
	defer close(releaseSend)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		resume, err := registry.PauseAssignments(ctx, testAgentID, 7)
		if resume != nil {
			resume()
		}
		result <- err
	}()
	waitForPausedAdmission(t, session)
	cancel()
	if err := waitForLifecycleResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled pause = %v", err)
	}
	if !session.AssignmentsAllowed() {
		t.Fatal("cancelled pause retained its admission hold")
	}
	select {
	case <-sent:
		t.Fatal("pause terminated an already admitted send")
	default:
	}
}

// Rationale: preparation includes claims and plan resolution, not just the
// final send; an idle check must not overtake an in-flight durable claim.
func TestRegistryAssignmentPauseDrainsDispatchPreparation(t *testing.T) {
	registry := NewRegistry()
	session := openAdmissionSession(t, registry, 7)
	finish, allowed := session.beginDispatch()
	if !allowed {
		t.Fatal("initial dispatch was rejected")
	}
	finished := false
	defer func() {
		if !finished {
			finish()
		}
	}()
	result := make(chan admissionPauseResult, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		resume, err := registry.PauseAssignments(ctx, testAgentID, 7)
		result <- admissionPauseResult{resume: resume, err: err}
	}()
	waitForPausedAdmission(t, session)
	select {
	case result := <-result:
		if result.resume != nil {
			result.resume()
		}
		t.Fatalf("pause overtook admitted dispatch: %v", result.err)
	default:
	}
	assertUnrelatedRegistryOpen(t, registry)
	finish()
	finished = true
	paused := <-result
	if paused.err != nil {
		t.Fatal(paused.err)
	}
	paused.resume()
	assertAdmissionAllowed(t, session)
}

type admissionPauseResult struct {
	resume func()
	err    error
}

func waitForPausedAdmission(t *testing.T, session *Session) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for session.AssignmentsAllowed() {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("assignment admission was not paused")
		}
	}
}

func openAdmissionSession(t *testing.T, registry *Registry, generation uint64) *Session {
	t.Helper()
	session, err := registry.Open(context.Background(), testAgentID, generation)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.Close)
	return session
}

func assertAdmissionAllowed(t *testing.T, session *Session) {
	t.Helper()
	if !session.AssignmentsAllowed() {
		t.Fatal("session does not allow assignments")
	}
	sent, err := session.sendAssignment(func() error { return nil })
	if err != nil || !sent {
		t.Fatalf("assignment after resume: sent=%t, error=%v", sent, err)
	}
}
