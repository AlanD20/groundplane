package etcd

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Native acceptance must not expose executable work without its captured run,
// Config cursor and operation ownership. Content authority is supplied through
// the trusted resolver seam; this does not execute capture/upload or a restore.
func TestNativeBackupConfigPublicationIsAtomicAndReplayable(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		lostReply bool
	}{{name: "normal"}, {name: "lost committed reply", lostReply: true}} {
		t.Run(scenario.name, func(t *testing.T) {
			fixture := newNativeBackupConfigPublicationFixture(t)
			if scenario.lostReply {
				repository, err := newBackupRuntimeRepository(
					&backupRuntimeUnknownOutcomeStore{hierarchyStore: fixture.store, failNext: true},
				)
				if err != nil {
					t.Fatal(err)
				}
				fixture.repository = repository
			}
			first := fixture.prepare(t)
			second := fixture.prepare(t)
			selected := first.prepared.Publication.Record()
			selected.Sources[0].TargetID = ids.New(ids.KindEnvironment)
			selected.Sources[0].Snapshot.Config.ReadRevision++
			retained := first.prepared.Publication.Record()
			if retained.Sources[0].TargetID != fixture.environmentID ||
				retained.Sources[0].Snapshot.Config.ReadRevision != fixture.readRevision {
				t.Fatal("reading the prepared run exposed mutable source authority")
			}
			result, err := first.prepared.Publication.PublishWork(
				t.Context(),
				first.task,
				first.item,
				first.input,
				first.marker,
			)
			if scenario.lostReply {
				if !isKind(err, errs.KindStorageUnavailable) {
					t.Fatalf("committed publication response was not lost: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if outcome, _, conflict, err := result.Classify(); err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
					t.Fatalf("native Backup publication = %v, %v, %v", outcome, conflict, err)
				}
			}
			view, err := fixture.queue.ReadWork(t.Context(), first.task.ID, first.item.ID, 0)
			if err != nil || !view.Task.Task.PublicationComplete || view.Task.Task.Scope != first.task.Scope ||
				view.Item.Item.Phase != workexecution.Ready || view.Item.Item.AttemptNumber != 0 {
				t.Fatalf("native Backup did not publish ready work: %#v, %v", view, err)
			}
			for _, key := range []string{
				backupruntime.BackupRunKey(first.task.ID), backupwork.InputKey(first.item.ID),
				hierarchy.EnvironmentOperationLockKey(first.task.Scope.EnvironmentID),
				hierarchy.EnvironmentMutationEpochKey(first.task.Scope.EnvironmentID),
				backupconfiguration.BackupConfigSnapshotKey(first.task.ID),
				backupconfiguration.BackupConfigSnapshotTaskReferenceKey(first.task.ID, first.task.ID),
				backupconfiguration.BackupConfigSnapshotReferenceTaskKey(first.task.ID, first.task.ID),
			} {
				entry := mustOptionalKey(t, fixture.store, key)
				if entry == nil || entry.ModRevision != view.Task.Revision || view.Item.Revision != view.Task.Revision {
					t.Fatalf("Backup companion did not join native publication: %s", key)
				}
			}
			owner, err := backupwork.NewInputRepository(fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			captured, _, err := owner.Load(t.Context(), view)
			if err != nil || !proto.Equal(captured, first.input) {
				t.Fatalf("captured native Backup input changed: %v", err)
			}
			run, err := fixture.repository.GetBackupRun(t.Context(), first.task.ID)
			if err != nil || run.Record.State != backupruntime.BackupRunQueued || run.Revision != view.Task.Revision ||
				run.Record.Sources[0].Snapshot.Config.ReadRevision != fixture.readRevision {
				t.Fatalf("Backup acceptance invented effects or changed its snapshot: %#v, %v", run, err)
			}
			before := fixture.store.revision
			if _, err := first.prepared.Publication.PublishWork(t.Context(), first.task, first.item, first.input, first.marker); !isKind(
				err,
				errs.KindStateConflict,
			) ||
				fixture.store.revision != before {
				t.Fatalf("consumed publication wrote again: %v", err)
			}
			replay, err := second.prepared.Publication.PublishWork(
				t.Context(),
				second.task,
				second.item,
				second.input,
				second.marker,
			)
			if err != nil {
				t.Fatal(err)
			}
			outcome, receipt, conflict, err := replay.Classify()
			if err != nil || conflict != nil || outcome != IdempotencyKnownExisting ||
				receipt.TaskID != first.task.ID ||
				fixture.store.revision != before {
				t.Fatalf("native Backup replay = %v, %v, %v", outcome, conflict, err)
			}
			if value := mustOptionalKey(t, fixture.store, workqueue.TaskKey(second.task.ID)); value != nil {
				t.Fatal("replay published another Task")
			}
			page, err := fixture.queue.ListTasks(t.Context(), "all", "", 100, 0)
			if err != nil || len(page.Tasks) != 1 {
				t.Fatalf("Backup created hidden Tasks: %#v, %v", page, err)
			}

		})
	}
}

// Prepared publication cannot outlive a policy/host change, or let its caller
// substitute another remote object after the source selection was captured.
func TestNativeBackupConfigPublicationRejectsChangedAuthority(t *testing.T) {
	for _, change := range []string{"policy", "host", "captured object"} {
		t.Run(change, func(t *testing.T) {
			fixture := newNativeBackupConfigPublicationFixture(t)
			capture := fixture.prepare(t)
			wanted := errs.KindStateConflict
			switch change {
			case "policy":
				policy := mustOptionalKey(t, fixture.store, backuppolicy.BackupPolicyKey(fixture.environmentID))
				coordination := mustOptionalKey(t, fixture.store, environmentcoordination.Key(fixture.environmentID))
				transition, err := fixture.store.Transact(t.Context(), []keyvalue.Condition{
					{Key: policy.Key, ModRevision: policy.ModRevision},
					{Key: coordination.Key, ModRevision: coordination.ModRevision},
				}, []keyvalue.Mutation{
					{Type: keyvalue.MutationPut, Key: policy.Key, Value: policy.Value},
					{Type: keyvalue.MutationPut, Key: coordination.Key, Value: coordination.Value},
				})
				if err != nil || !transition.Succeeded {
					t.Fatalf("policy/coordination transition = %t, %v", transition.Succeeded, err)
				}
			case "host":
				connectorDeletionDeleteKey(t, fixture.store, hostbindings.HostKey(capture.prepared.HostID))
			case "captured object":
				capture.input.Steps[0].GetCapture().Target.ObjectKey += ".changed"
				wanted = errs.KindValidationFailed
			}
			before := fixture.store.revision
			result, err := capture.prepared.Publication.PublishWork(
				t.Context(),
				capture.task,
				capture.item,
				capture.input,
				capture.marker,
			)
			if err == nil {
				_, _, conflict, classifyErr := result.Classify()
				if classifyErr != nil {
					t.Fatal(classifyErr)
				}
				err = conflict
			}
			if !isKind(err, wanted) || fixture.store.revision != before {
				t.Fatalf("changed authority publication = %v", err)
			}
			for _, key := range []string{workqueue.TaskKey(capture.task.ID), workqueue.ItemKey(capture.item.ID),
				backupwork.InputKey(capture.item.ID), backupruntime.BackupRunKey(capture.task.ID),
				backupconfiguration.BackupConfigSnapshotKey(capture.task.ID)} {
				if mustOptionalKey(t, fixture.store, key) != nil {
					t.Fatalf("rejected Backup published %s", key)
				}
			}
		})
	}
}

type nativeBackupConfigPublicationFixture struct {
	store         *memoryHierarchyStore
	repository    *BackupRuntimeRepository
	queue         *workqueue.Repository
	environmentID string
	readRevision  int64
	at            time.Time
}

type nativeBackupConfigCapture struct {
	prepared PreparedManualBackupRun
	task     workexecution.Task
	item     workexecution.Item
	input    *agentpb.BackupWorkInput
	marker   idempotency.IdempotencyMarker
}

func newNativeBackupConfigPublicationFixture(t *testing.T) nativeBackupConfigPublicationFixture {
	t.Helper()
	repository, store, seeded := newBackupRuntimeBareFixture(t)
	configureBackupRuntimeConfigRun(t, store, &seeded)
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	fixture := nativeBackupConfigPublicationFixture{store: store, repository: repository, queue: queue,
		environmentID: seeded.EnvironmentID, at: seeded.CreatedAt}
	fixture.enroll(t, ids.New(ids.KindAgent))
	fixture.readRevision = store.revision
	return fixture
}

func (fixture nativeBackupConfigPublicationFixture) enroll(t *testing.T, agentID string) {
	t.Helper()
	change, err := hostbindings.PrepareLocalEnrollment(t.Context(), fixture.store, agentID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.queue.Commit(t.Context(), change)
	if err != nil || !result.Succeeded {
		t.Fatalf("enroll Backup host = %t, %v", result.Succeeded, err)
	}
}

func (fixture nativeBackupConfigPublicationFixture) prepare(t *testing.T) nativeBackupConfigCapture {
	t.Helper()
	taskID := ids.New(ids.KindTask)
	prepared, err := fixture.repository.PrepareManualBackupRun(t.Context(), backupplanning.ManualBackupRunInput{
		EnvironmentID: fixture.environmentID, TaskID: taskID, OperationID: ids.New(ids.KindOperation),
		PlanID: ids.New(ids.KindPlan), FixedRevision: fixture.readRevision, CreatedAt: fixture.at,
		ResolveConfig: func(_ context.Context, source backupplanning.BackupConfigSnapshotInput) (*agentpb.BackupConfigCaptureAuthority, error) {
			if source.EnvironmentID != fixture.environmentID || source.TaskID != taskID ||
				source.ReadRevision != fixture.readRevision {
				t.Fatal("Config resolver escaped its captured scope")
			}
			return &agentpb.BackupConfigCaptureAuthority{
				EnvironmentId:            source.EnvironmentID,
				MetadataSnapshotRevision: source.ReadRevision,
				Content: &agentpb.BackupConfigContentAuthority{ManifestSha256: bytes.Repeat([]byte{1}, 32),
					MetadataSnapshotSha256: bytes.Repeat(
						[]byte{2},
						32,
					), ManifestSizeBytes: 128, SourceSizeBytes: 4096},
			}, nil
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prepared.Publication.Clear)
	input, raw, reference, err := executionplan.CaptureBackupWorkInput(ids.New(ids.KindPlan), &agentpb.BackupWorkInput{
		Schema: 1, Operation: agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE,
		ProjectId: prepared.Scope.ProjectId, Project: prepared.Scope.Project,
		EnvironmentId: prepared.Scope.EnvironmentId, Environment: prepared.Scope.Environment,
		Services: prepared.Scope.Services, Steps: prepared.Authority, Artifacts: prepared.Artifacts,
	})
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	task := workexecution.Task{ID: taskID, Actor: workexecution.OperatorActor,
		Scope: workexecution.Scope{
			TenantID:      prepared.Owner.TenantID,
			ProjectID:     prepared.Owner.ProjectID,
			EnvironmentID: prepared.Owner.EnvironmentID,
		},
		Action: workexecution.ActionBackup, TargetID: fixture.environmentID, TargetName: fixture.environmentID,
		Intent: reference, ExpectedItems: 1, CreatedAt: fixture.at, UpdatedAt: fixture.at, Deadline: fixture.at.Add(6 * time.Hour)}
	item, err := backupwork.NewItem(task, ids.New(ids.KindWorkItem), prepared.HostID, input)
	if err != nil {
		t.Fatal(err)
	}
	marker := testDirectMarker()
	marker.Kind, marker.State, marker.TaskID = idempotency.IdempotencyMarkerTask, idempotency.IdempotencyMarkerPending, task.ID
	marker.Locator = idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID: fixture.environmentID, Method: "POST", Route: "/environments/{id}/backup-run", Key: "native-backup-config-publication"}
	marker.CreatedAt, marker.UpdatedAt, marker.TerminalAt, marker.RetainUntil = fixture.at, fixture.at, time.Time{}, time.Time{}
	marker.Response.Status, marker.Response.Body = 202, []byte(`{"task_id":"`+task.ID+`"}`)
	return nativeBackupConfigCapture{prepared: prepared, task: task, item: item, input: input, marker: marker}
}
