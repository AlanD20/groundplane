package etcd

import (
	"context"
	"net/http"
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
// runtime intent and replay receipt; overlapping publication fragments previously
// duplicated the Task/item comparisons and rejected every action before execution.
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
				CreatedAt: now,
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
			for attempt := range 2 {
				result, err := repository.PublishServiceLifecycle(
					ctx,
					&tenant,
					project,
					environment,
					current,
					input,
					nil,
					now.Add(time.Minute),
					marker,
				)
				if err != nil {
					t.Fatalf("publication %d: %v", attempt, err)
				}
				outcome, _, conflict, err := result.Classify()
				want := IdempotencyKnownApplied
				if attempt == 1 {
					want = IdempotencyKnownExisting
				}
				if err != nil || conflict != nil || outcome != want {
					t.Fatalf("publication %d: %v / %v / %v", attempt, outcome, conflict, err)
				}
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
