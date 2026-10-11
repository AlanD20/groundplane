package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// QA: BP-19. Rationale: the parent deadline must durably stop selection across
// restart without declaring unfinished child effects gone or overwriting an
// earlier operator Abort; a restored failed child cannot replace that outcome.
func TestBlueprintParentDeadlinePreservesStopOutcomeUntilRestoration(t *testing.T) {
	for _, abortFirst := range []bool{false, true} {
		name := "deadline-first"
		if abortFirst {
			name = "abort-first"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newBlueprintParentFailureFixture(t)
			deadline := fixture.parent.StartedAt.Add(time.Duration(fixture.parent.TimeoutSeconds) * time.Second)
			if err := fixture.repository.RequestBlueprintParentTimeout(
				fixture.ctx, fixture.parent.ID, deadline.Add(-time.Nanosecond),
			); err == nil {
				t.Fatal("premature timeout accepted")
			}
			if outcome, err := fixture.repository.BlueprintParentStopRequested(fixture.ctx, fixture.parent.ID); err != nil ||
				outcome != "" {
				t.Fatalf("premature timeout changed stop state: %s, %v", outcome, err)
			}
			want := taskjournal.TaskStatusTimedOut
			if abortFirst {
				want = taskjournal.TaskStatusAborted
				if err := fixture.repository.RequestBlueprintParentAbort(fixture.ctx, fixture.parent.ID,
					fixture.now.Add(2*time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			if err := fixture.repository.RequestBlueprintParentTimeout(fixture.ctx, fixture.parent.ID, deadline); err != nil {
				t.Fatal(err)
			}
			// Reconstruct the repository: stop intent is durable, not an in-memory timer.
			resumed, err := newTaskRepository(fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			if outcome, err := resumed.BlueprintParentStopRequested(fixture.ctx, fixture.parent.ID); err != nil ||
				outcome != want {
				t.Fatalf("resumed outcome = %s, %v; want %s", outcome, err, want)
			}
			if _, err := resumed.TimeoutBlueprintParent(fixture.ctx, fixture.parent.Owner.EnvironmentID,
				fixture.parent.ID, deadline); err == nil {
				t.Fatal("timeout released unsettled child effects or overwrote Abort")
			}
			changed := fixture.desired
			changed.Complete = true
			if _, err := resumed.PublishBlueprintDesiredPlan(fixture.ctx, fixture.parent.ID, changed); err == nil {
				t.Fatal("stop gate permitted further desired-plan publication")
			}
			at := deadline.Add(time.Second)
			conditions, mutations, err := resumed.prepareBlueprintChildReceipt(
				fixture.ctx, fixture.child, fixture.assignment, taskjournal.TaskStatusFailed, true, at,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ZeroMutationBytes(mutations)
			result, err := fixture.store.Transact(fixture.ctx, conditions, mutations)
			if err != nil || !result.Succeeded {
				t.Fatalf("restored child publication = %#v, %v", result, err)
			}
			terminal, err := resumed.finishStoppedBlueprintParent(fixture.ctx,
				fixture.parent.Owner.EnvironmentID, fixture.parent.ID, want, at.Add(time.Second))
			if err != nil || terminal.Record.Status != want {
				t.Fatalf("settled parent = %s, %v; want %s", terminal.Record.Status, err, want)
			}
		})
	}
}
