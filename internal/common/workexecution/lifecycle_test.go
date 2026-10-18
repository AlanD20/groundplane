package workexecution

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// TASK-20: redelivery must not renew Start permission or run work on another host.
func TestNativeStartRedeliveryPreservesHostAndDeadline(t *testing.T) {
	item, identity, at := readyNativeWork()
	wrongHost := identity
	wrongHost.HostID = ids.DeriveAt(ids.KindHost, at, item.TaskID, "other-host")
	if _, _, err := Assign(item, wrongHost, nativeAttemptID(item, at, "wrong-host"), Forward, at.Add(time.Second), item.Deadline); err == nil {
		t.Fatal("work was assigned to a different host")
	}
	assigned, attempt, err := Assign(
		item,
		identity,
		nativeAttemptID(item, at, "forward"),
		Forward,
		at.Add(time.Second),
		item.Deadline,
	)
	if err != nil {
		t.Fatal(err)
	}
	running, started, err := Start(assigned, attempt, at.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	replayed, receipt, err := Start(running, started, at.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ID != started.ID || receipt.Number != 1 || !receipt.StartedAt.Equal(*started.StartedAt) ||
		!receipt.Deadline.Equal(started.Deadline) || !replayed.UpdatedAt.Equal(running.UpdatedAt) {
		t.Fatal("redelivery changed the execution identity, start time or deadline")
	}
}

// TASK-21: supersession may permit captured cleanup, never replay forward work
// or replace its original failed result with successful cleanup.
func TestNativeCleanupPreservesOriginalFailureAfterSupersession(t *testing.T) {
	item, identity, at := readyNativeWork()
	assigned, forward, err := Assign(
		item,
		identity,
		nativeAttemptID(item, at, "forward"),
		Forward,
		at.Add(time.Second),
		item.Deadline,
	)
	if err != nil {
		t.Fatal(err)
	}
	running, forward, err := Start(assigned, forward, at.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	failure := Input{ID: ids.DeriveAt(ids.KindPlan, at, item.ID, "failed-result"), SHA256: strings.Repeat("b", 64)}
	failed, recorded, err := Finish(
		running,
		forward,
		Completion{Outcome: Failed, Evidence: failure},
		at.Add(3*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := RevokeForward(failed, Superseded, at.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	decision := RetryDecision{
		Direction: Forward,
		Result:    failure,
		NotBefore: at.Add(5 * time.Second),
		Deadline:  at.Add(time.Minute),
	}
	if _, err := Retry(revoked, recorded, decision, at.Add(5*time.Second)); err == nil {
		t.Fatal("superseded forward execution was permitted to retry")
	}
	decision.Direction = Recovery
	queued, err := Retry(revoked, recorded, decision, at.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	assignedCleanup, cleanup, err := Assign(
		queued,
		identity,
		nativeAttemptID(item, at, "cleanup"),
		Recovery,
		at.Add(6*time.Second),
		queued.Deadline,
	)
	if err != nil {
		t.Fatal(err)
	}
	cleaning, cleanup, err := Start(assignedCleanup, cleanup, at.Add(7*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	proof := Input{ID: ids.DeriveAt(ids.KindPlan, at, item.ID, "cleanup-result"), SHA256: strings.Repeat("c", 64)}
	settled, _, err := Finish(
		cleaning,
		cleanup,
		Completion{Outcome: Succeeded, Evidence: proof, EffectsSettled: true},
		at.Add(8*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Phase != Settled || settled.Cleanup != CleanupComplete || settled.Outcome != Failed ||
		settled.FirstFailure == nil || *settled.FirstFailure != failure || settled.FirstFailureAttempt != recorded.Number ||
		settled.Result == nil || *settled.Result != failure || settled.CleanupResult == nil || *settled.CleanupResult != proof ||
		!settled.ForwardRevoked || *settled.Input != *item.Input || settled.TaskID != item.TaskID || settled.ID != item.ID {
		t.Fatal("cleanup changed the failed action, captured input or retained failure identity")
	}
	if recorded.Outcome != Failed || recorded.Result == nil || *recorded.Result != failure {
		t.Fatal("cleanup mutated the original failed attempt")
	}
}

func readyNativeWork() (Item, Identity, time.Time) {
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	id := func(kind ids.Kind, label string) string { return ids.DeriveAt(kind, at, "native-lifecycle", label) }
	host := id(ids.KindHost, "host")
	item := Item{
		ID: id(ids.KindWorkItem, "work"), TaskID: id(ids.KindTask, "task"), Operation: Release,
		TargetID: id(ids.KindService, "service"), Placement: Placement{Executor: Agent, HostID: host},
		Claims: []Claim{{HostID: host, ScopeID: id(ids.KindService, "service")}},
		Input:  &Input{ID: id(ids.KindPlan, "input"), SHA256: strings.Repeat("a", 64)},
		Phase:  Ready, Cleanup: CleanupNone, CreatedAt: at, UpdatedAt: at, Deadline: at.Add(2 * time.Minute),
	}
	return item, Identity{Executor: Agent, HostID: host, AgentID: id(ids.KindAgent, "agent"), Generation: 1}, at
}

func nativeAttemptID(item Item, at time.Time, label string) string {
	return ids.DeriveAt(ids.KindAttempt, at, item.ID, label)
}
