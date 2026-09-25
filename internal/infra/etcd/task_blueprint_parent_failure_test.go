package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: an exactly restored failed child must atomically stop further
// publication and fail its visible Apply, while an earlier operator Abort
// remains the authoritative terminal outcome.
func TestBlueprintFailedChildSettlesClaimAndStopsParent(t *testing.T) {
	tests := []struct {
		name          string
		terminal      taskjournal.TaskStatus
		abortFirst    bool
		superseded    bool
		expectFailure bool
	}{
		{name: "failure-first", terminal: taskjournal.TaskStatusFailed, expectFailure: true},
		{name: "abort-first", terminal: taskjournal.TaskStatusFailed, abortFirst: true},
		{name: "unrequested-aborted", terminal: taskjournal.TaskStatusAborted, expectFailure: true},
		{name: "superseded-aborted", terminal: taskjournal.TaskStatusAborted, superseded: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBlueprintParentFailureFixture(t)
			if test.abortFirst {
				if err := fixture.repository.RequestBlueprintParentAbort(
					fixture.ctx,
					fixture.parent.ID,
					fixture.now.Add(2*time.Second),
				); err != nil {
					t.Fatalf("request parent Abort = %v", err)
				}
			}
			if test.superseded {
				replacement, err := idempotency.EncodeTaskReference(ids.NewAt(ids.KindTask, fixture.now, 109))
				if err != nil {
					t.Fatal(err)
				}
				transaction, err := fixture.store.Transact(fixture.ctx, nil, []keyvalue.Mutation{{
					Type:  keyvalue.MutationPut,
					Key:   blueprints.EnvironmentBlueprintHeadKey(fixture.parent.Owner.EnvironmentID),
					Value: replacement,
				}})
				if err != nil || !transaction.Succeeded {
					t.Fatalf("supersede parent = %#v, %v", transaction, err)
				}
			}
			conditions, mutations, err := fixture.repository.prepareBlueprintChildReceipt(
				fixture.ctx,
				fixture.child,
				fixture.assignment,
				test.terminal,
				true,
				fixture.now.Add(3*time.Second),
			)
			if err != nil {
				t.Fatalf("prepare failed child receipt = %v", err)
			}
			defer keyvalue.ZeroMutationBytes(mutations)
			transaction, err := fixture.store.Transact(fixture.ctx, conditions, mutations)
			if err != nil || !transaction.Succeeded {
				t.Fatalf("commit failed child receipt = %#v, %v", transaction, err)
			}
			ledger, err := blueprintunits.NewRepository(fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := ledger.Load(fixture.ctx, fixture.parent.Owner.EnvironmentID)
			if err != nil || len(snapshot.Executions) != 0 {
				t.Fatalf("settled child execution = %#v, %v", snapshot.Executions, err)
			}
			failed, err := fixture.repository.BlueprintParentFailureRequested(
				fixture.ctx,
				fixture.parent.ID,
			)
			if err != nil || failed != test.expectFailure {
				t.Fatalf("parent failure marker = %t, %v", failed, err)
			}
			sealed := fixture.desired
			sealed.Complete = true
			if _, err := fixture.repository.PublishBlueprintDesiredPlan(
				fixture.ctx,
				fixture.parent.ID,
				sealed,
			); err == nil {
				t.Fatal("parent published desired work after its stop gate closed")
			}
			if test.abortFirst {
				terminal, err := fixture.repository.AbortBlueprintParent(
					fixture.ctx,
					fixture.parent.Owner.EnvironmentID,
					fixture.parent.ID,
					fixture.now.Add(4*time.Second),
				)
				if err != nil || terminal.Record.Status != taskjournal.TaskStatusAborted {
					t.Fatalf("Abort-first parent terminal = %#v, %v", terminal, err)
				}
				return
			}
			if test.superseded {
				terminal, err := fixture.repository.RetireSupersededBlueprintParent(
					fixture.ctx,
					fixture.parent.Owner.EnvironmentID,
					fixture.parent.ID,
					fixture.now.Add(4*time.Second),
				)
				if err != nil || terminal.Record.Status != taskjournal.TaskStatusAborted {
					t.Fatalf("superseded parent terminal = %#v, %v", terminal, err)
				}
				return
			}
			terminal, err := fixture.repository.FailBlueprintParent(
				fixture.ctx,
				fixture.parent.Owner.EnvironmentID,
				fixture.parent.ID,
				fixture.now.Add(4*time.Second),
			)
			if err != nil || terminal.Record.Status != taskjournal.TaskStatusFailed {
				t.Fatalf("failed parent terminal = %#v, %v", terminal, err)
			}
		})
	}
}

type blueprintParentFailureFixture struct {
	ctx        context.Context
	store      *memoryTaskStore
	repository *TaskRepository
	now        time.Time
	parent     TaskRecord
	child      TaskRecord
	assignment taskassignments.TaskAssignmentRecord
	desired    blueprintunits.DesiredPlan
}

func newBlueprintParentFailureFixture(t *testing.T) blueprintParentFailureFixture {
	t.Helper()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := taskJournalTime()
	parent := validTaskRecord(now)
	parent.Executor = taskjournal.TaskExecutorBlueprint
	parent.Owner.ProjectID = ids.NewAt(ids.KindProject, now, 101)
	parent.Owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, now, 102)
	parent.Target = parent.Owner.EnvironmentID
	parent.Params = map[string]string{blueprints.EnvironmentDesiredRevisionParam: parent.ID}
	parent.Steps = nil
	marker := pendingTaskMarker(parent)
	marker.Locator.ScopeID = parent.Owner.EnvironmentID
	parent.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	parent, err = TransitionTaskStatus(
		parent,
		taskjournal.TaskStatusPending,
		taskjournal.TaskStatusRunning,
		now.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	parentValue, err := EncodeTaskRecord(parent)
	if err != nil {
		t.Fatal(err)
	}
	parentReference, err := idempotency.EncodeTaskReference(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	markerKey, err := idempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	markerValue, err := idempotency.EncodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatal(err)
	}
	target := blueprintunits.ResourceKey{Kind: ids.KindService, ID: ids.NewAt(ids.KindService, now, 103)}
	unit := blueprintunits.Unit{
		Target: target, Fingerprint: strings.Repeat("b", 64),
		Writes: []blueprintunits.ResourceKey{target},
	}
	desired := blueprintunits.DesiredPlan{
		EnvironmentID: parent.Owner.EnvironmentID,
		ParentTaskID:  parent.ID,
		Units:         []blueprintunits.Unit{unit},
	}
	desiredValue, err := blueprintunits.EncodeDesiredPlan(desired)
	if err != nil {
		t.Fatal(err)
	}
	epochValue, err := blueprintunits.EncodeEpoch(blueprintunits.EpochRecord{
		EnvironmentID: parent.Owner.EnvironmentID,
		Sequence:      1,
	})
	if err != nil {
		t.Fatal(err)
	}
	child := validTaskRecord(now)
	child.ID = ids.NewAt(ids.KindTask, now, 104)
	child.OperationID = ids.NewAt(ids.KindOperation, now, 105)
	child.PlanID = ids.NewAt(ids.KindPlan, now, 106)
	child.Actor = taskjournal.TaskActorSystem
	child.Owner = parent.Owner
	child.Target = parent.Owner.EnvironmentID
	publicationID := ids.NewULID()
	child.Params = map[string]string{
		taskjournal.TaskBlueprintParentParam:      parent.ID,
		releaserender.TaskReleasePublicationParam: publicationID,
	}
	executionValue, err := blueprintunits.EncodeExecution(blueprintunits.ExecutionRecord{
		EnvironmentID: parent.Owner.EnvironmentID,
		ParentTaskID:  parent.ID,
		TaskID:        child.ID,
		PlanID:        child.PlanID,
		Epoch:         1,
		State:         blueprintunits.Running,
		Unit:          unit,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestValue, err := releases.EncodeReleaseRecord("release-staged-manifest", releases.ReleaseStagedManifest{
		PublicationID: publicationID,
		OperationID:   child.OperationID,
		Digest:        strings.Repeat("a", 64),
		Members: []releases.ReleaseStagedMemberRef{{
			ServiceID: target.ID,
			ReleaseID: ids.NewAt(ids.KindDeployment, now, 107),
		}},
		CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	absence, err := blueprintunits.PrepareInitialAbsencePublication(
		parent.Owner.EnvironmentID,
		parent.ID,
		[]blueprintunits.ResourceKey{target},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer absence.Clear()
	seedMutations := append([]keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskStorageKey(parent.ID), Value: parentValue},
		{Type: keyvalue.MutationPut, Key: taskjournal.BlueprintParentClaimKey(parent.ID), Value: parentReference},
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskActiveOperationKey(parent.OperationID), Value: parentReference},
		{Type: keyvalue.MutationPut, Key: markerKey, Value: markerValue},
		{Type: keyvalue.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(parent.Owner.EnvironmentID), Value: parentReference},
		{Type: keyvalue.MutationPut, Key: blueprintunits.DesiredPlanKey(parent.Owner.EnvironmentID), Value: desiredValue},
		{Type: keyvalue.MutationPut, Key: blueprintunits.EpochKey(parent.Owner.EnvironmentID), Value: epochValue},
		{Type: keyvalue.MutationPut, Key: blueprintunits.ExecutionKey(parent.Owner.EnvironmentID, child.PlanID), Value: executionValue},
		{Type: keyvalue.MutationPut, Key: releases.ReleaseManifestStagingKey(publicationID), Value: manifestValue},
	}, absence.Mutations()...)
	seed, err := store.Transact(ctx, absence.Conditions(), seedMutations)
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed failed child fixture = %#v, %v", seed, err)
	}
	return blueprintParentFailureFixture{
		ctx: ctx, store: store, repository: repository, now: now,
		parent: parent, child: child,
		assignment: taskassignments.TaskAssignmentRecord{
			AssignmentID:   ids.NewAt(ids.KindAssignment, now, 108),
			ExecutionEpoch: 1,
		},
		desired: desired,
	}
}
