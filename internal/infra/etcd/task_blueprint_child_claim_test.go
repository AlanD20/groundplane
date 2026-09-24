package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: an Agent may start a private unit only while its visible parent
// is the current head. Stale children must not starve unrelated queued work.
func TestBlueprintChildAssignmentFencesCurrentParentAndPendingUnit(t *testing.T) {
	for _, current := range []bool{true, false} {
		t.Run(map[bool]string{true: "current", false: "overtaken"}[current], func(t *testing.T) {
			ctx := context.Background()
			store := newMemoryTaskStore()
			repository, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			now := taskJournalTime()
			environmentID := ids.NewAt(ids.KindEnvironment, now, 50)
			projectID := ids.NewAt(ids.KindProject, now, 51)
			parent := validTaskRecord(now)
			parent.Executor = testtaskjournal.TaskExecutorBlueprint
			parent.Owner.ProjectID = projectID
			parent.Owner.EnvironmentID = environmentID
			parent.Target = environmentID
			parent.Params = map[string]string{testblueprints.EnvironmentDesiredRevisionParam: parent.ID}
			parent.Steps = nil
			parent, err = TransitionTaskStatus(
				parent, testtaskjournal.TaskStatusPending, testtaskjournal.TaskStatusRunning, now.Add(time.Second),
			)
			if err != nil {
				t.Fatal(err)
			}
			parentValue, err := EncodeTaskRecord(parent)
			if err != nil {
				t.Fatal(err)
			}
			parentRef, err := testidempotency.EncodeTaskReference(parent.ID)
			if err != nil {
				t.Fatal(err)
			}
			child := validTaskRecord(now)
			child.ID = ids.NewAt(ids.KindTask, now, 52)
			child.OperationID = ids.NewAt(ids.KindOperation, now, 53)
			child.PlanID = ids.NewAt(ids.KindPlan, now, 54)
			child.Actor = testtaskjournal.TaskActorSystem
			child.Owner = parent.Owner
			child.Target = environmentID
			child.Params = map[string]string{testtaskjournal.TaskBlueprintParentParam: parent.ID}
			childMarker := pendingTaskMarker(child)
			child.idempotencyMarker = cloneIdempotencyLocator(&childMarker.Locator)
			childMarkerValue, err := testidempotency.EncodeIdempotencyMarker(childMarker)
			if err != nil {
				t.Fatal(err)
			}
			childMarkerKey, err := testidempotency.IdempotencyMarkerKey(childMarker.Locator)
			if err != nil {
				t.Fatal(err)
			}
			childValue, err := EncodeTaskRecord(child)
			if err != nil {
				t.Fatal(err)
			}
			childRef, err := testidempotency.EncodeTaskReference(child.ID)
			if err != nil {
				t.Fatal(err)
			}
			target := blueprintunits.ResourceKey{Kind: ids.KindService, ID: ids.NewAt(ids.KindService, now, 55)}
			execution := blueprintunits.ExecutionRecord{
				EnvironmentID: environmentID, ParentTaskID: parent.ID, TaskID: child.ID,
				PlanID: child.PlanID, State: blueprintunits.Pending,
				Unit: blueprintunits.Unit{Target: target, Fingerprint: strings.Repeat("a", 64), Writes: []blueprintunits.ResourceKey{target}},
			}
			executionValue, err := blueprintunits.EncodeExecution(execution)
			if err != nil {
				t.Fatal(err)
			}
			epochValue, err := blueprintunits.EncodeEpoch(blueprintunits.EpochRecord{
				EnvironmentID: environmentID, Sequence: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			headID := parent.ID
			if !current {
				headID = ids.NewAt(ids.KindTask, now, 56)
			}
			headRef, err := testidempotency.EncodeTaskReference(headID)
			if err != nil {
				t.Fatal(err)
			}
			mutations := []testkeyvalue.Mutation{
				{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(parent.ID), Value: parentValue},
				{Type: testkeyvalue.MutationPut, Key: testtaskjournal.BlueprintParentClaimKey(parent.ID), Value: parentRef},
				{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(child.ID), Value: childValue},
				{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskQueueKey(child.Executor, child.ID), Value: childRef},
				{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskActiveOperationKey(child.OperationID), Value: childRef},
				{Type: testkeyvalue.MutationPut, Key: childMarkerKey, Value: childMarkerValue},
				{Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintHeadKey(environmentID), Value: headRef},
				{Type: testkeyvalue.MutationPut, Key: blueprintunits.ExecutionKey(environmentID, child.PlanID), Value: executionValue},
				{Type: testkeyvalue.MutationPut, Key: blueprintunits.EpochKey(environmentID), Value: epochValue},
			}
			ordinaryID := ""
			if !current {
				ordinary := validTaskRecord(now)
				ordinary.ID = ids.NewAt(ids.KindTask, now, 60)
				ordinary.OperationID = ids.NewAt(ids.KindOperation, now, 61)
				ordinaryMarker := pendingTaskMarker(ordinary)
				ordinary.idempotencyMarker = cloneIdempotencyLocator(&ordinaryMarker.Locator)
				ordinaryValue, err := EncodeTaskRecord(ordinary)
				if err != nil {
					t.Fatal(err)
				}
				ordinaryRef, err := testidempotency.EncodeTaskReference(ordinary.ID)
				if err != nil {
					t.Fatal(err)
				}
				ordinaryID = ordinary.ID
				mutations = append(mutations,
					testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(ordinary.ID), Value: ordinaryValue},
					testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskQueueKey(ordinary.Executor, ordinary.ID), Value: ordinaryRef},
					testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskActiveOperationKey(ordinary.OperationID), Value: ordinaryRef},
				)
			}
			seed, err := store.Transact(ctx, nil, mutations)
			if err != nil || !seed.Succeeded {
				t.Fatalf("seed Blueprint child = %#v, %v", seed, err)
			}
			agentID := ids.NewAt(ids.KindAgent, now, 57)
			assignment, found, err := repository.ClaimNextTask(ctx, agentID, 1, now.Add(2*time.Second))
			if err != nil || !found {
				t.Fatalf("claim Blueprint child = %#v, %t, %v", assignment, found, err)
			}
			stored, err := store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: []string{
				blueprintunits.ExecutionKey(environmentID, child.PlanID),
				testtaskjournal.TaskQueueKey(child.Executor, child.ID),
			}})
			if err != nil || stored == nil || len(stored.Values) != 2 {
				t.Fatalf("read child execution = %#v, %v", stored, err)
			}
			persisted, err := blueprintunits.DecodeExecution(stored.Values[0].Value)
			if err != nil {
				t.Fatal(err)
			}
			if current && (assignment.Task.Record.ID != child.ID || persisted.State != blueprintunits.Running ||
				persisted.Epoch != 1 || stored.Values[1] != nil) {
				t.Fatalf("current child authority = %#v, %#v", assignment, persisted)
			}
			if current {
				_, err := repository.AcknowledgeTask(
					ctx, agentID, 1, child.ID, assignment.Assignment.Record.AssignmentID,
					testtaskjournal.TaskStatusFailed,
					testtaskjournal.TaskResultRecord{
						Kind: testtaskjournal.TaskResultCompose, ExitCode: 1,
						Diagnostic: testtaskjournal.TaskResultDiagnosticComposeFailed,
					},
					now.Add(3*time.Second),
				)
				if err != nil {
					t.Fatalf("acknowledge failed child = %v", err)
				}
				ledger, err := blueprintunits.NewRepository(store)
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := ledger.Load(ctx, environmentID)
				if err != nil || len(snapshot.Executions) != 1 ||
					snapshot.Executions[0].Record.State != blueprintunits.Draining {
					t.Fatalf("failed child effect claim = %#v, %v", snapshot, err)
				}
			}
			if !current && (assignment.Task.Record.ID != ordinaryID ||
				persisted.State != blueprintunits.Pending || stored.Values[1] == nil) {
				t.Fatalf("overtaken child blocked ordinary work: %#v, %#v", assignment, persisted)
			}
		})
	}
}
