package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type nativePruneFixture struct {
	repository *BackupRuntimeRepository
	unselected []keyvalue.Versioned[backupruntime.BackupRecoveryPointPruneRecord]
	store      *memoryHierarchyStore
	queue      *workqueue.Repository
	owner      *backupwork.InputRepository
	task       workexecution.Task
	item       workexecution.Item
	input      *agentpb.BackupWorkInput
	agentID    string
	points     []backupruntime.BackupRecoveryPointRecord
}

// Verified objects and retention selection are supplied; publication, queue
// assignment, Start, checkpoints and joined settlement use their native owners.
func newNativePruneFixture(t *testing.T, count int) nativePruneFixture {
	t.Helper()
	repository, store, run := newBackupRuntimeBareFixture(t)
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := backupwork.NewInputRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	fixture := nativePruneFixture{
		repository: repository,
		store:      store,
		queue:      queue,
		owner:      owner,
		agentID:    ids.New(ids.KindAgent),
	}
	enrollment, err := hostbindings.PrepareLocalEnrollment(t.Context(), store, fixture.agentID)
	if err != nil {
		t.Fatal(err)
	}
	fixture.commit(t, enrollment)
	pending := make([]keyvalue.Versioned[backupruntime.BackupRecoveryPointPruneRecord], count)
	for index := range pending {
		source := run.Sources[0]
		source.RecoveryPointID = ids.NewAt(ids.KindRecoveryPoint, source.RecoveryPointCreatedAt, int64(7800+index))
		source.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + source.SourceID + "/" + source.RecoveryPointID + "/artifact.bin"
		source.State, source.Phase = backupruntime.BackupSourceAttemptStaged, backupruntime.BackupSourcePhaseUpload
		backupRuntimeCompleteSourceArtifact(run, &source)
		point := backupRuntimeTestPoint(run, source, run.CreatedAt.Add(time.Second))
		revision := seedBackupRuntimePointAuthority(t, store, point)
		fixture.points = append(fixture.points, point)
		sweep := backupruntime.BackupRetentionSweepRecord{
			SourceID: point.SourceID, TriggerRecoveryPointID: point.ID, Keep: 3,
			Revision: run.PolicyRevision, PolicySHA256: run.PolicySHA256,
			State: backupruntime.BackupRetentionCompleted, SelectionRevision: revision,
			PruneOperationID: run.OperationID, CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt.Add(time.Second),
		}
		encoded, err := backupruntime.EncodeBackupRetentionSweepRecord(sweep)
		if err != nil {
			t.Fatal(err)
		}
		sweepKey := backupruntime.BackupRetentionKey(point.SourceID, point.ID)
		firstSweep := fixture.commit(
			t,
			workqueue.Change{
				Conditions: []keyvalue.Condition{{Key: backupruntime.BackupRetentionKey(point.SourceID, point.ID)}},
				Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut,
					Key: backupruntime.BackupRetentionKey(point.SourceID, point.ID), Value: encoded}},
			},
		)
		encoded, err = backupruntime.EncodeBackupRetentionSweepRecord(sweep)
		if err != nil {
			t.Fatal(err)
		}
		fixture.commit(
			t,
			workqueue.Change{Conditions: []keyvalue.Condition{{Key: sweepKey, ModRevision: firstSweep.Revision}},
				Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: sweepKey, Value: encoded}}},
		)
		prune := backupruntime.BackupRecoveryPointPruneRecord{Point: point.BackupRecoveryPointSnapshot,
			PointRevision: revision, PolicyRevision: run.PolicyRevision, PolicySHA256: run.PolicySHA256,
			OperationID: run.OperationID, State: backupruntime.BackupPrunePending,
			CreatedAt: run.CreatedAt.Add(2 * time.Second), UpdatedAt: run.CreatedAt.Add(2 * time.Second)}
		encoded, err = backupruntime.EncodeBackupRecoveryPointPruneRecord(prune)
		if err != nil {
			t.Fatal(err)
		}
		commit := fixture.commit(
			t,
			workqueue.Change{
				Conditions: []keyvalue.Condition{{Key: backupruntime.BackupRecoveryPointPruneKey(point.ID)}},
				Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut,
					Key: backupruntime.BackupRecoveryPointPruneKey(point.ID), Value: encoded}},
			},
		)
		pending[index] = keyvalue.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
			Record:   prune,
			Revision: commit.Revision,
		}
	}
	for index := range pending {
		pending[index].ReadRevision = store.revision
	}
	return fixture.accept(t, pending, run.CreatedAt.Add(3*time.Second))
}

func (fixture nativePruneFixture) accept(
	t *testing.T,
	pending []keyvalue.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
	at time.Time,
) nativePruneFixture {
	t.Helper()
	fixture.unselected = nil
	for len(pending) > 0 {
		prepared, err := fixture.repository.PrepareBackupPrune(t.Context(), pending, ids.New(ids.KindTask), at)
		if err != nil {
			t.Fatal(err)
		}
		defer prepared.Publication.Clear()
		task, item, input, marker := nativeBackupPrunePublicationFixture(t, prepared)
		before := fixture.store.revision
		fits, err := prepared.Publication.FitsWork(t.Context(), task, item, input, marker)
		if err != nil {
			t.Fatal(err)
		}
		if fixture.store.revision != before {
			t.Fatal("prune preflight wrote state")
		}
		if !fits {
			fixture.unselected = append(fixture.unselected, pending[len(pending)-1])
			pending = pending[:len(pending)-1]
			continue
		}
		result, err := prepared.Publication.PublishWork(t.Context(), task, item, input, marker)
		if err != nil {
			t.Fatal(err)
		}
		outcome, _, conflict, err := result.Classify()
		if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
			t.Fatalf("publish native prune: %v/%v/%v", outcome, conflict, err)
		}
		fixture.task, fixture.item, fixture.input = task, item, input
		fixture.points = fixture.points[:len(pending)]
		return fixture
	}
	t.Fatal("no Recovery Point fits native prune publication")
	return nativePruneFixture{}
}

func (fixture nativePruneFixture) commit(t *testing.T, changes ...workqueue.Change) keyvalue.TransactionResult {
	t.Helper()
	for _, change := range changes {
		defer keyvalue.ClearMutationValues(change.Mutations)
	}
	result, err := fixture.queue.Commit(t.Context(), changes...)
	if err != nil || !result.Succeeded {
		t.Fatalf("native prune transaction: %t, %v", result.Succeeded, err)
	}
	return result
}

func (fixture nativePruneFixture) start(t *testing.T) workqueue.ResumeView {
	t.Helper()
	registration, executor, err := workqueue.PrepareRegistration(workqueue.ExecutorSnapshot{}, workqueue.ExecutorRecord{
		Identity: workexecution.Identity{
			Executor: workexecution.Agent,
			HostID:   fixture.item.Placement.HostID,
			AgentID:  fixture.agentID,
		},
		ProcessGeneration: [16]byte{1}, Capacity: 1, Operations: []workexecution.Operation{workexecution.BackupPrune}})
	if err != nil {
		t.Fatal(err)
	}
	fixture.commit(t, registration)
	current, err := fixture.queue.ReadExecutor(t.Context(), fixture.item.Placement)
	if err != nil {
		t.Fatal(err)
	}
	available, err := workqueue.PrepareAvailability(current, executor.Identity, true, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	fixture.commit(t, available)
	page, err := fixture.queue.ScanReady(t.Context(), fixture.item.Placement, "")
	if err != nil || len(page.Entries) != 1 {
		t.Fatalf("prune ready: %v/%v", page, err)
	}
	at := fixture.task.CreatedAt.Add(time.Second)
	candidate, err := fixture.queue.ReadCandidate(t.Context(), page.Entries[0], executor.Identity, at, nil)
	if err != nil || candidate.Wait != nil || candidate.Stopped {
		t.Fatalf("prune candidate: %v/%v", candidate, err)
	}
	_, admission, err := fixture.owner.PrepareAdmission(t.Context(), workqueue.View{
		Task: candidate.Assignment.Task, Item: candidate.Assignment.Item, ReadRevision: candidate.Assignment.ReadRevision})
	if err != nil {
		t.Fatal(err)
	}
	request := candidate.Assignment
	request.AttemptID, request.At, request.Deadline = ids.New(ids.KindAttempt), at, fixture.task.Deadline
	assignment, attempt, err := workqueue.PrepareAssignment(request)
	if err != nil {
		t.Fatal(err)
	}
	fixture.commit(t, assignment, admission)
	view, err := fixture.queue.ReadResume(t.Context(), fixture.task.ID, fixture.item.ID, attempt.ID, executor.Identity)
	if err != nil {
		t.Fatal(err)
	}
	_, guarded, err := fixture.owner.LoadAssignment(t.Context(), view)
	if err != nil {
		t.Fatal(err)
	}
	current, err = fixture.queue.ReadExecutor(t.Context(), fixture.item.Placement)
	if err != nil {
		t.Fatal(err)
	}
	start, err := workqueue.PrepareStart(view.Work.Task, view.Work.Item, view.Attempt, current, at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	fixture.commit(t, guarded, start)
	return fixture.resume(t, view)
}

func (fixture nativePruneFixture) resume(t *testing.T, previous workqueue.ResumeView) workqueue.ResumeView {
	t.Helper()
	view, err := fixture.queue.ReadResume(
		t.Context(),
		fixture.task.ID,
		fixture.item.ID,
		previous.Attempt.Attempt.ID,
		previous.Attempt.Attempt.Executor,
	)
	if err != nil {
		t.Fatal(err)
	}
	return view
}
