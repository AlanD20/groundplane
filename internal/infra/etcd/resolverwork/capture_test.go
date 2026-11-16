package resolverwork

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// A forward failure must remain readable after cleanup renews its deadline;
// otherwise neither the failure acknowledgement nor recovery can be delivered.
func TestCapturedWorkSurvivesCleanupAndExplicitRetry(t *testing.T) {
	for _, budget := range []time.Duration{time.Minute, 20 * time.Minute} {
		t.Run(budget.String(), func(t *testing.T) {
			at := time.Now().UTC()
			input := resolverexecution.Input{
				TaskID: ids.New(ids.KindTask), WorkID: ids.New(ids.KindWorkItem),
				HostID: ids.New(ids.KindHost), ComponentID: ids.New(ids.KindComponent),
				OperationID: ids.New(ids.KindOperation), BaselineSHA256: strings.Repeat("a", 64),
				Generation: 1, CreatedAt: at, Deadline: at.Add(8 * time.Minute),
			}
			_, reference, err := resolverexecution.Capture(ids.New(ids.KindPlan), input)
			if err != nil {
				t.Fatal(err)
			}
			task := workexecution.Task{ID: input.TaskID, Actor: workexecution.SystemActor,
				Action: workexecution.ActionUpdate, TargetID: input.ComponentID, TargetName: "CoreDNS",
				Intent: reference, CreatedAt: at, UpdatedAt: at, Deadline: input.Deadline,
				ExpectedItems: 1, PublicationComplete: true, Progress: workexecution.Progress{Ready: 1}}
			item := workexecution.Item{ID: input.WorkID, TaskID: input.TaskID, Operation: workexecution.Resolver,
				TargetID: input.ComponentID, Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: input.HostID},
				Claims: []workexecution.Claim{{HostID: input.HostID, ScopeID: input.ComponentID}}, Input: &reference,
				Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone, CreatedAt: at, UpdatedAt: at, Deadline: input.Deadline}
			check := func(item workexecution.Item) {
				t.Helper()
				if err := matchesWork(input, reference, task, item); err != nil {
					t.Fatalf("captured resolver lost during %s/%s: %v", item.Phase, item.Cleanup, err)
				}
			}
			check(item)
			wrong := workexecution.Clone(item)
			wrong.Deadline = at.Add(time.Minute)
			if matchesWork(input, reference, task, wrong) == nil {
				t.Fatal("initial publication accepted a different forward deadline")
			}
			identity := workexecution.Identity{Executor: workexecution.Agent, HostID: input.HostID,
				AgentID: ids.New(ids.KindAgent), Generation: 1}
			assigned, forward, err := workexecution.Assign(item, identity, ids.New(ids.KindAttempt),
				workexecution.Forward, at.Add(time.Second), item.Deadline)
			if err != nil {
				t.Fatal(err)
			}
			running, forward, err := workexecution.Start(assigned, forward, at.Add(2*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			failure := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("b", 64)}
			failed, forward, err := workexecution.Finish(running, forward,
				workexecution.Completion{Outcome: workexecution.Failed, Evidence: failure}, at.Add(3*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			queued, err := workexecution.Retry(failed, forward, workexecution.RetryDecision{
				Direction: workexecution.Recovery, Result: failure, NotBefore: at.Add(4 * time.Second),
				Deadline: at.Add(budget)}, at.Add(4*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			task.UpdatedAt = at.Add(4 * time.Second)
			check(queued)
			assigned, cleanup, err := workexecution.Assign(queued, identity, ids.New(ids.KindAttempt),
				workexecution.Recovery, at.Add(5*time.Second), queued.Deadline)
			if err != nil {
				t.Fatal(err)
			}
			cleaning, cleanup, err := workexecution.Start(assigned, cleanup, at.Add(6*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			check(cleaning)
			proof := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("c", 64)}
			settled, cleanup, err := workexecution.Finish(
				cleaning,
				cleanup,
				workexecution.Completion{
					Outcome:        workexecution.Succeeded,
					Evidence:       proof,
					EffectsSettled: true,
				},
				at.Add(7*time.Second),
			)
			if err != nil {
				t.Fatal(err)
			}
			check(settled)
			retried, err := workexecution.Retry(settled, cleanup, workexecution.RetryDecision{
				Direction: workexecution.Forward, Result: proof, NotBefore: at.Add(8 * time.Second),
				Deadline: at.Add(30 * time.Minute)}, at.Add(8*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			task.Deadline = retried.Deadline
			check(retried)
			wrong = workexecution.Clone(retried)
			wrong.Placement.HostID = ids.New(ids.KindHost)
			if matchesWork(input, reference, task, wrong) == nil {
				t.Fatal("renewed deadline authorized another host")
			}
			_, after, err := resolverexecution.Capture(reference.ID, input)
			if err != nil || after != reference || forward.Result == nil || *forward.Result != failure {
				t.Fatal("cleanup changed the captured input or original failure")
			}
		})
	}
}
