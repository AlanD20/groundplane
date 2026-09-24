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
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: an overtaken running Apply remains visible until its private
// effects are settled; only then may the parent and replay marker retire.
func TestRetireSupersededBlueprintParentRequiresSettledChildren(t *testing.T) {
	for _, unsettled := range []bool{true, false} {
		t.Run(map[bool]string{true: "unsettled", false: "settled"}[unsettled], func(t *testing.T) {
			ctx := context.Background()
			store := newMemoryTaskStore()
			repository, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			now := taskJournalTime()
			parent := validTaskRecord(now)
			parent.Executor = testtaskjournal.TaskExecutorBlueprint
			parent.Owner.ProjectID = ids.NewAt(ids.KindProject, now, 40)
			parent.Owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, now, 41)
			parent.Target = parent.Owner.EnvironmentID
			parent.Params = map[string]string{testblueprints.EnvironmentDesiredRevisionParam: parent.ID}
			parent.Steps = nil
			marker := pendingTaskMarker(parent)
			parent.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
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
			markerValue, err := testidempotency.EncodeIdempotencyMarker(marker)
			if err != nil {
				t.Fatal(err)
			}
			markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
			if err != nil {
				t.Fatal(err)
			}
			parentRef, err := testidempotency.EncodeTaskReference(parent.ID)
			if err != nil {
				t.Fatal(err)
			}
			newHeadID := ids.NewAt(ids.KindTask, now, 42)
			newHeadRef, err := testidempotency.EncodeTaskReference(newHeadID)
			if err != nil {
				t.Fatal(err)
			}
			mutations := []testkeyvalue.Mutation{
				{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(parent.ID), Value: parentValue},
				{Type: testkeyvalue.MutationPut, Key: testtaskjournal.BlueprintParentClaimKey(parent.ID), Value: parentRef},
				{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskActiveOperationKey(parent.OperationID), Value: parentRef},
				{Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintHeadKey(parent.Owner.EnvironmentID), Value: newHeadRef},
				{Type: testkeyvalue.MutationPut, Key: markerKey, Value: markerValue},
			}
			if unsettled {
				target := blueprintunits.ResourceKey{Kind: ids.KindService, ID: ids.NewAt(ids.KindService, now, 43)}
				execution := blueprintunits.ExecutionRecord{
					EnvironmentID: parent.Owner.EnvironmentID,
					ParentTaskID:  parent.ID,
					TaskID:        ids.NewAt(ids.KindTask, now, 44),
					PlanID:        ids.NewAt(ids.KindPlan, now, 45), Epoch: 1,
					Unit:  blueprintunits.Unit{Target: target, Fingerprint: strings.Repeat("a", 64), Writes: []blueprintunits.ResourceKey{target}},
					State: blueprintunits.Running,
				}
				executionValue, err := blueprintunits.EncodeExecution(execution)
				if err != nil {
					t.Fatal(err)
				}
				epochValue, err := blueprintunits.EncodeEpoch(blueprintunits.EpochRecord{
					EnvironmentID: parent.Owner.EnvironmentID, Sequence: 1,
				})
				if err != nil {
					t.Fatal(err)
				}
				mutations = append(mutations,
					testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: blueprintunits.ExecutionKey(parent.Owner.EnvironmentID, execution.PlanID), Value: executionValue},
					testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: blueprintunits.EpochKey(parent.Owner.EnvironmentID), Value: epochValue},
				)
			}
			seed, err := store.Transact(ctx, nil, mutations)
			if err != nil || !seed.Succeeded {
				t.Fatalf("seed parent = %#v, %v", seed, err)
			}
			retired, err := repository.RetireSupersededBlueprintParent(
				ctx, parent.Owner.EnvironmentID, parent.ID, now.Add(2*time.Second),
			)
			if unsettled {
				if kind, ok := errs.KindOf(err); !ok || kind != errs.KindResourceInUse {
					t.Fatalf("unsettled child retirement error = %v", err)
				}
				return
			}
			if err != nil || retired.Record.Status != testtaskjournal.TaskStatusAborted || retired.Record.FinishedAt == nil {
				t.Fatalf("retire settled parent = %#v, %v", retired, err)
			}
			read, err := store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: []string{
				testtaskjournal.BlueprintParentClaimKey(parent.ID),
				testtaskjournal.TaskActiveOperationKey(parent.OperationID),
				markerKey,
			}})
			if err != nil || read == nil || len(read.Values) != 3 || read.Values[0] != nil || read.Values[1] != nil {
				t.Fatalf("retired parent lifecycle = %#v, %v", read, err)
			}
			terminalMarker, err := testidempotency.DecodeIdempotencyMarker(read.Values[2].Value, marker.Locator)
			if err != nil || terminalMarker.State != testidempotency.IdempotencyMarkerFailed {
				t.Fatalf("retired parent replay marker = %#v, %v", terminalMarker, err)
			}
		})
	}
}
