package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Rationale: lifecycle admission must atomically publish captured input, work,
// runtime intent under the original pure accepted Task. Replay cannot create a
// second Task, and acceptance alone must not expose executable work.
func TestServiceLifecyclePublicationCommitsAndReplays(t *testing.T) {
	for _, action := range []workexecution.Action{workexecution.ActionStart, workexecution.ActionStop, workexecution.ActionDestroy} {
		t.Run(string(action), func(t *testing.T) {
			ctx := context.Background()
			_, store, environment, project, current := routeRepositoryTestHierarchy(t)
			hierarchy, err := newHierarchyRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			tenant, err := hierarchy.GetTenant(ctx, project.Record.TenantID)
			if err != nil {
				t.Fatal(err)
			}
			repository, err := newServiceRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			now := testMarkerTime()
			input := servicelifecyclework.Input{
				TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem), PlanID: ids.New(ids.KindPlan),
				Action: action, ServiceID: current.Record.Desired.ID, ServiceRevision: current.Revision,
				Scope: workexecution.Scope{
					TenantID:      tenant.Record.ID,
					ProjectID:     project.Record.ID,
					EnvironmentID: environment.Record.ID,
				},
				CreatedAt: now.Add(time.Second),
			}
			marker := testDirectMarker()
			marker.Kind, marker.State, marker.TaskID = idempotency.IdempotencyMarkerTask, idempotency.IdempotencyMarkerPending, input.TaskID
			marker.TerminalAt, marker.RetainUntil = time.Time{}, time.Time{}
			marker.Locator.ScopeKind, marker.Locator.ScopeID = idempotency.IdempotencyScopeEnvironment, environment.Record.ID
			marker.Locator.Method, marker.Locator.Route = http.MethodPost, "/services/{id}/"+string(action)
			marker.ReplayTarget = &idempotency.IdempotencyReplayTarget{
				Kind: idempotency.IdempotencyReplayTargetService,
				ID:   input.ServiceID,
			}
			marker.Response.Status, marker.Response.Body = http.StatusAccepted, []byte(`{"task_id":"`+input.TaskID+`"}`)
			queue, err := workqueue.NewRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			receipts, err := NewIdempotencyRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			pure := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)}
			task := workexecution.Task{
				ID:             input.TaskID,
				Actor:          workexecution.OperatorActor,
				Action:         action,
				Scope:          input.Scope,
				TargetID:       input.ServiceID,
				TargetName:     current.Record.Desired.Name,
				AcceptedIntent: &pure,
				CreatedAt:      now,
				UpdatedAt:      now,
				Deadline:       now.Add(24 * time.Hour),
			}
			for attempt := range 2 {
				acceptance, err := queue.PrepareAcceptedTask(ctx, task, marker)
				if err != nil {
					t.Fatal(err)
				}
				result, err := receipts.PublishAcceptedAction(ctx, marker, acceptance)
				keyvalue.ClearMutationValues(acceptance.Mutations)
				if err != nil {
					t.Fatalf("acceptance %d: %v", attempt, err)
				}
				outcome, _, conflict, err := result.Classify()
				want := IdempotencyKnownApplied
				if attempt == 1 {
					want = IdempotencyKnownExisting
				}
				if err != nil || conflict != nil || outcome != want {
					t.Fatalf("acceptance %d: %v / %v / %v", attempt, outcome, conflict, err)
				}
			}
			// Pure acceptance cannot change runtime intent or expose executable work.
			before, err := store.Get(ctx, workqueue.ItemKey(input.WorkItemID))
			if err != nil || before.Entry != nil {
				t.Fatalf("work existed before capture: %v", err)
			}
			unchanged, err := repository.GetService(ctx, input.ServiceID)
			if err != nil ||
				unchanged.Record.Runtime.UnconfirmedLifecycleTaskID != current.Record.Runtime.UnconfirmedLifecycleTaskID ||
				unchanged.Record.Runtime.RuntimeIntent != current.Record.Runtime.RuntimeIntent {
				t.Fatalf("pure lifecycle acceptance changed runtime intent: %v", err)
			}
			accepted, _, err := queue.ReadTask(ctx, task.ID, 0)
			if err != nil {
				t.Fatal(err)
			}
			publication, err := repository.PrepareServiceLifecycle(
				ctx,
				&tenant,
				project,
				environment,
				current,
				input,
				nil,
				input.CreatedAt.Add(time.Minute),
				accepted,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearMutationValues(publication.Change.Mutations)
			task = accepted.Task
			task.Intent, err = publication.Input.Reference()
			if err != nil {
				t.Fatal(err)
			}
			task.ExpectedItems, task.UpdatedAt = 1, input.CreatedAt
			promotion, err := queue.PrepareAcceptedTaskPromotion(
				ctx,
				accepted,
				task,
				[]workexecution.Item{publication.Item},
				nil,
				false,
				input.CreatedAt,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearMutationValues(promotion.Mutations)
			result, err := queue.Commit(ctx, promotion, publication.Change)
			keyvalue.ClearValues(result.FailureReads)
			if finishErr := publication.Finish(ctx, err == nil && result.Succeeded); finishErr != nil {
				t.Fatal(finishErr)
			}
			if err != nil || !result.Succeeded {
				t.Fatalf("native lifecycle promotion: %v / %v", result.Succeeded, err)
			}
			for _, key := range []string{workqueue.TaskKey(input.TaskID), workqueue.ItemKey(input.WorkItemID), servicelifecyclework.InputKey(input.WorkItemID)} {
				read, err := store.Get(ctx, key)
				if err != nil || read.Entry == nil {
					t.Fatalf("missing accepted lifecycle record %s: %v", key, err)
				}
			}
			updated, err := repository.GetService(ctx, input.ServiceID)
			want := core.ServiceRuntimeIntentRunning
			if action == workexecution.ActionStop {
				want = core.ServiceRuntimeIntentStopped
			}
			if action == workexecution.ActionDestroy {
				want = core.ServiceRuntimeIntentAbsent
			}
			if err != nil || updated.Record.Runtime.RuntimeIntent != want ||
				updated.Record.Runtime.UnconfirmedLifecycleTaskID != input.TaskID {
				t.Fatalf("accepted runtime intent was not saved: %v", err)
			}
		})
	}
}
