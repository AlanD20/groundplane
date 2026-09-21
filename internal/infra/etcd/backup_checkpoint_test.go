package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: only the exact active assignment can advance the contiguous
// checkpoint cursor, and replay is accepted only for the original digest.
func TestBackupCheckpointPlanFencesAssignmentSequenceAndDigest(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeBareFixture(t)
	input, revision := seedBackupCheckpointAssignment(t, store, run)
	binding := backupRunCheckpointBinding(run, 0)
	plan, err := repository.loadBackupCheckpointPlan(context.Background(), input, revision, binding)
	if err != nil || plan.duplicate {
		t.Fatalf("loadBackupCheckpointPlan() = %#v, %v", plan, err)
	}
	marker := "/v1/test/backup-checkpoint-domain/" + input.TaskID
	conditions, mutations, err := plan.composeTransaction(
		[]testkeyvalue.Condition{{Key: marker}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: marker, Value: []byte(plan.digest)}},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := repository.TransactRuntime(context.Background(), conditions, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	plan.clear()
	if err != nil || !result.Succeeded {
		t.Fatalf("commit checkpoint = %#v, %v", result, err)
	}
	replay, err := repository.loadBackupCheckpointPlan(
		context.Background(), input, result.Revision, binding,
	)
	if err != nil || !replay.duplicate {
		t.Fatalf("loadBackupCheckpointPlan(replay) = %#v, %v", replay, err)
	}
	changed := input
	changed.Payload.StoredSizeBytes++
	if _, err := repository.loadBackupCheckpointPlan(
		context.Background(), changed, result.Revision, binding,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("loadBackupCheckpointPlan(changed digest) error = %v", err)
	}
	next := input
	next.Sequence++
	next.Payload.Kind = testbackupruntime.BackupCheckpointSourceCleanupCompleted
	next.Payload.StoredSizeBytes = 0
	next.Payload.StoredSHA256 = ""
	nextPlan, err := repository.loadBackupCheckpointPlan(
		context.Background(), next, result.Revision, binding,
	)
	if err != nil {
		t.Fatalf("loadBackupCheckpointPlan(next sequence) error = %v", err)
	}
	nextConditions, nextMutations, err := nextPlan.composeTransaction(
		[]testkeyvalue.Condition{{Key: marker, ModRevision: result.Revision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: marker, Value: []byte(nextPlan.digest)}},
	)
	if err != nil {
		t.Fatal(err)
	}
	nextResult, err := repository.TransactRuntime(context.Background(), nextConditions, nextMutations)
	testkeyvalue.ClearMutationValues(nextMutations)
	nextPlan.clear()
	if err != nil || !nextResult.Succeeded {
		t.Fatalf("commit next checkpoint = %#v, %v", nextResult, err)
	}
	if _, err := repository.loadBackupCheckpointPlan(
		context.Background(), input, nextResult.Revision, binding,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("loadBackupCheckpointPlan(old sequence) error = %v", err)
	}
	dedupKey := testbackupruntime.BackupCheckpointDedupKey(next)
	dedup := mustOptionalKey(t, store, dedupKey)
	rewritten, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: dedupKey, ModRevision: dedup.ModRevision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: dedupKey, Value: dedup.Value}},
	)
	if err != nil || !rewritten.Succeeded {
		t.Fatalf("rewrite checkpoint dedupe = %#v, %v", rewritten, err)
	}
	if _, err := repository.loadBackupCheckpointPlan(
		context.Background(), next, rewritten.Revision, binding,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("loadBackupCheckpointPlan(rewritten dedupe) error = %v", err)
	}
}

// Rationale: removing any active-assignment companion makes an old Agent
// checkpoint unauthorized before domain mutations are constructed.
func TestBackupCheckpointPlanRejectsStaleAssignment(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeBareFixture(t)
	input, revision := seedBackupCheckpointAssignment(t, store, run)
	binding := backupRunCheckpointBinding(run, 0)
	claimKey := testtaskjournal.TaskAssignmentKey(input.AgentID, input.TaskID)
	claim := mustOptionalKey(t, store, claimKey)
	result, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: claimKey, ModRevision: claim.ModRevision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: claimKey}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("remove assignment = %#v, %v", result, err)
	}
	if _, err := repository.loadBackupCheckpointPlan(
		context.Background(), input, result.Revision, binding,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("loadBackupCheckpointPlan(stale assignment) error = %v", err)
	}
	if marker := mustOptionalKey(
		t,
		store,
		"/v1/test/backup-checkpoint-domain/"+input.TaskID,
	); marker != nil {
		t.Fatalf("stale checkpoint domain marker = %#v at initial revision %d", marker, revision)
	}
}

// Rationale: a valid checkpoint payload for one ordered source or prune point
// cannot borrow another ordered step's assignment-fenced cursor.
func TestBackupCheckpointPlanRejectsCrossStepSubstitution(t *testing.T) {
	t.Parallel()

	t.Run("backup source", func(t *testing.T) {
		repository, store, run := newBackupRuntimeBareFixture(t)
		run.Sources = append(
			run.Sources,
			testBackupLaterSource(run.CreatedAt, 1, testbackupruntime.BackupSourceAttemptPending),
		)
		input, revision := seedBackupCheckpointAssignment(t, store, run)
		input.Payload.PointID = run.Sources[1].RecoveryPointID
		if _, err := repository.loadBackupCheckpointPlan(
			context.Background(), input, revision, backupRunCheckpointBinding(run, 1),
		); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Fatalf("loadBackupCheckpointPlan(cross-source step) error = %v", err)
		}
	})

	t.Run("prune point", func(t *testing.T) {
		repository, store, run := newBackupRuntimeBareFixture(t)
		pointIDs := []string{
			run.Sources[0].RecoveryPointID,
			ids.NewAt(ids.KindRecoveryPoint, run.CreatedAt.Add(time.Millisecond), 804),
		}
		input, revision := seedBackupPruneCheckpointAssignment(t, store, run, pointIDs)
		input.Payload = testbackupruntime.BackupCheckpointPayload{
			Kind: testbackupruntime.BackupCheckpointRemoteObjectAbsent, PointID: pointIDs[1],
		}
		if _, err := repository.loadBackupCheckpointPlan(
			context.Background(), input, revision,
			backupCheckpointBinding{taskType: testtaskjournal.TaskBackupPrune, ordinal: 1, pointID: pointIDs[1]},
		); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Fatalf("loadBackupCheckpointPlan(cross-prune step) error = %v", err)
		}
	})
}

// Rationale: persistence deduplication and Agent delivery must share exactly
// one canonical digest grammar for every current checkpoint kind, including
// upload-completed orphan evidence.
func TestBackupCheckpointDigestMatchesExecutionPlanGrammarForEveryKind(t *testing.T) {
	t.Parallel()
	pointID := "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	restoreGenerationID := "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	storedDigest := bytes.Repeat([]byte{0x11}, 32)
	decodedDigest := bytes.Repeat([]byte{0x22}, 32)
	stagedDigest := bytes.Repeat([]byte{0x33}, 32)
	liveDigest := bytes.Repeat([]byte{0x44}, 32)
	manifestDigest := bytes.Repeat([]byte{0x55}, 32)
	tests := []struct {
		name    string
		payload testbackupruntime.BackupCheckpointPayload
		request *agentpb.BackupCheckpointRequest
	}{
		{
			name: "artifact prepared",
			payload: testbackupruntime.BackupCheckpointPayload{
				Kind:            testbackupruntime.BackupCheckpointArtifactPrepared,
				PointID:         pointID,
				StoredSizeBytes: 101,
				StoredSHA256:    hex.EncodeToString(storedDigest),
			},
			request: &agentpb.BackupCheckpointRequest{
				Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_ARTIFACT_PREPARED,
				Payload: &agentpb.BackupCheckpointRequest_ArtifactPrepared{
					ArtifactPrepared: &agentpb.BackupArtifactPreparedCheckpoint{
						PointId: pointID, StoredSizeBytes: 101, StoredSha256: storedDigest,
					},
				},
			},
		},
		{
			name: "upload verified",
			payload: testbackupruntime.BackupCheckpointPayload{
				Kind:            testbackupruntime.BackupCheckpointUploadVerified,
				PointID:         pointID,
				StoredSizeBytes: 102,
				StoredSHA256:    hex.EncodeToString(storedDigest),
			},
			request: &agentpb.BackupCheckpointRequest{
				Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_UPLOAD_VERIFIED,
				Payload: &agentpb.BackupCheckpointRequest_UploadVerified{
					UploadVerified: &agentpb.BackupUploadVerifiedCheckpoint{
						PointId: pointID, StoredSizeBytes: 102, StoredSha256: storedDigest,
					},
				},
			},
		},
		{
			name: "source cleanup completed",
			payload: testbackupruntime.BackupCheckpointPayload{
				Kind:    testbackupruntime.BackupCheckpointSourceCleanupCompleted,
				PointID: pointID,
			},
			request: &agentpb.BackupCheckpointRequest{
				Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_SOURCE_CLEANUP_COMPLETED,
				Payload: &agentpb.BackupCheckpointRequest_SourceCleanupCompleted{
					SourceCleanupCompleted: &agentpb.BackupSourceCleanupCompletedCheckpoint{PointId: pointID},
				},
			},
		},
		{
			name: "restore artifact validated",
			payload: testbackupruntime.BackupCheckpointPayload{
				Kind:          testbackupruntime.BackupCheckpointRestoreArtifactValidated,
				PointID:       pointID,
				StoredSHA256:  hex.EncodeToString(storedDigest),
				DecodedSHA256: hex.EncodeToString(decodedDigest),
			},
			request: &agentpb.BackupCheckpointRequest{
				Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_RESTORE_ARTIFACT_VALIDATED,
				Payload: &agentpb.BackupCheckpointRequest_RestoreArtifactValidated{
					RestoreArtifactValidated: &agentpb.BackupRestoreArtifactValidatedCheckpoint{
						PointId: pointID, StoredSha256: storedDigest, DecodedSha256: decodedDigest,
					},
				},
			},
		},
		{
			name: "volume tree staged",
			payload: testbackupruntime.BackupCheckpointPayload{
				Kind:                     testbackupruntime.BackupCheckpointVolumeTreeStaged,
				PointID:                  pointID,
				StagedTreeManifestSHA256: hex.EncodeToString(stagedDigest),
			},
			request: &agentpb.BackupCheckpointRequest{
				Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_VOLUME_TREE_STAGED,
				Payload: &agentpb.BackupCheckpointRequest_VolumeTreeStaged{
					VolumeTreeStaged: &agentpb.BackupVolumeTreeStagedCheckpoint{
						PointId: pointID, StagedTreeManifestSha256: stagedDigest,
					},
				},
			},
		},
		{
			name: "volume tree exchanged",
			payload: testbackupruntime.BackupCheckpointPayload{
				Kind:                   testbackupruntime.BackupCheckpointVolumeTreeExchanged,
				PointID:                pointID,
				LiveTreeManifestSHA256: hex.EncodeToString(liveDigest),
			},
			request: &agentpb.BackupCheckpointRequest{
				Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_VOLUME_TREE_EXCHANGED,
				Payload: &agentpb.BackupCheckpointRequest_VolumeTreeExchanged{
					VolumeTreeExchanged: &agentpb.BackupVolumeTreeExchangedCheckpoint{
						PointId: pointID, LiveTreeManifestSha256: liveDigest,
					},
				},
			},
		},
		{
			name: "volume replaced tree cleaned",
			payload: testbackupruntime.BackupCheckpointPayload{
				Kind:    testbackupruntime.BackupCheckpointVolumeReplacedTreeCleaned,
				PointID: pointID,
			},
			request: &agentpb.BackupCheckpointRequest{
				Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_VOLUME_REPLACED_TREE_CLEANED,
				Payload: &agentpb.BackupCheckpointRequest_VolumeReplacedTreeCleaned{
					VolumeReplacedTreeCleaned: &agentpb.BackupVolumeReplacedTreeCleanedCheckpoint{PointId: pointID},
				},
			},
		},
		{
			name: "config generation staged",
			payload: testbackupruntime.BackupCheckpointPayload{
				Kind:                          testbackupruntime.BackupCheckpointConfigGenerationStaged,
				PointID:                       pointID,
				RestoreGenerationID:           restoreGenerationID,
				EntryGenerationManifestSHA256: hex.EncodeToString(manifestDigest),
			},
			request: &agentpb.BackupCheckpointRequest{
				Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_CONFIG_GENERATION_STAGED,
				Payload: &agentpb.BackupCheckpointRequest_ConfigGenerationStaged{
					ConfigGenerationStaged: &agentpb.BackupConfigGenerationStagedCheckpoint{
						PointId: pointID, RestoreGenerationId: restoreGenerationID,
						EntryGenerationManifestSha256: manifestDigest,
					},
				},
			},
		},
		{
			name: "config generation activated",
			payload: testbackupruntime.BackupCheckpointPayload{
				Kind:                testbackupruntime.BackupCheckpointConfigGenerationActivated,
				PointID:             pointID,
				RestoreGenerationID: restoreGenerationID,
				RenderGeneration:    106,
			},
			request: &agentpb.BackupCheckpointRequest{
				Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_CONFIG_GENERATION_ACTIVATED,
				Payload: &agentpb.BackupCheckpointRequest_ConfigGenerationActivated{
					ConfigGenerationActivated: &agentpb.BackupConfigGenerationActivatedCheckpoint{
						PointId: pointID, RestoreGenerationId: restoreGenerationID, RenderGeneration: 106,
					},
				},
			},
		},
		{
			name: "postgres restore verified",
			payload: testbackupruntime.BackupCheckpointPayload{
				Kind:    testbackupruntime.BackupCheckpointPostgresRestoreVerified,
				PointID: pointID,
			},
			request: &agentpb.BackupCheckpointRequest{
				Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_POSTGRES_RESTORE_VERIFIED,
				Payload: &agentpb.BackupCheckpointRequest_PostgresRestoreVerified{
					PostgresRestoreVerified: &agentpb.BackupPostgresRestoreVerifiedCheckpoint{PointId: pointID},
				},
			},
		},
		{
			name: "remote object absent",
			payload: testbackupruntime.BackupCheckpointPayload{
				Kind:    testbackupruntime.BackupCheckpointRemoteObjectAbsent,
				PointID: pointID,
			},
			request: &agentpb.BackupCheckpointRequest{
				Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_REMOTE_OBJECT_ABSENT,
				Payload: &agentpb.BackupCheckpointRequest_RemoteObjectAbsent{
					RemoteObjectAbsent: &agentpb.BackupRemoteObjectAbsentCheckpoint{PointId: pointID},
				},
			},
		},
		{
			name: "upload completed",
			payload: testbackupruntime.BackupCheckpointPayload{
				Kind:            testbackupruntime.BackupCheckpointUploadCompleted,
				PointID:         pointID,
				StoredSizeBytes: 107,
				StoredSHA256:    hex.EncodeToString(storedDigest),
			},
			request: &agentpb.BackupCheckpointRequest{
				Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_UPLOAD_COMPLETED,
				Payload: &agentpb.BackupCheckpointRequest_UploadCompleted{
					UploadCompleted: &agentpb.BackupUploadCompletedCheckpoint{
						PointId: pointID, StoredSizeBytes: 107, StoredSha256: storedDigest,
					},
				},
			},
		},
	}
	if got, want := len(tests), int(testbackupruntime.BackupCheckpointUploadCompleted); got != want {
		t.Fatalf("checkpoint parity vectors = %d, want %d current kinds", got, want)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := testbackupruntime.BackupCheckpointDigest(test.payload)
			if err != nil {
				t.Fatalf("backupCheckpointDigest() error = %v", err)
			}
			want, err := executionplan.ComputeBackupCheckpointPayloadDigest(test.request)
			if err != nil {
				t.Fatalf("ComputeBackupCheckpointPayloadDigest() error = %v", err)
			}
			if got != hex.EncodeToString(want) {
				t.Fatalf("backup checkpoint digest = %s, want %x", got, want)
			}
		})
	}
}

func seedBackupCheckpointAssignment(
	t *testing.T,
	store *memoryHierarchyStore,
	run testbackupruntime.BackupRunRecord,
) (testbackupruntime.BackupCheckpointInput, int64) {
	pointIDs := make([]string, len(run.Sources))
	for index := range run.Sources {
		pointIDs[index] = run.Sources[index].RecoveryPointID
	}
	return seedBackupCheckpointAssignmentForTask(t, store, run, testtaskjournal.TaskBackup, pointIDs)
}

func seedBackupPruneCheckpointAssignment(
	t *testing.T,
	store *memoryHierarchyStore,
	run testbackupruntime.BackupRunRecord,
	pointIDs []string,
) (testbackupruntime.BackupCheckpointInput, int64) {
	return seedBackupCheckpointAssignmentForTask(t, store, run, testtaskjournal.TaskBackupPrune, pointIDs)
}

func seedBackupCheckpointAssignmentForTask(
	t *testing.T,
	store *memoryHierarchyStore,
	run testbackupruntime.BackupRunRecord,
	taskType testtaskjournal.TaskType,
	pointIDs []string,
) (testbackupruntime.BackupCheckpointInput, int64) {
	t.Helper()
	now := run.CreatedAt
	task := validTaskRecord(now)
	task.ID = run.TaskID
	task.OperationID = run.OperationID
	task.Type = taskType
	if taskType == testtaskjournal.TaskBackupPrune {
		task.Actor = testtaskjournal.TaskActorSystem
	}
	task.Target = run.EnvironmentID
	task.RenderGeneration = 0
	task.Params = nil
	task.Materializations = nil
	task.TimeoutSeconds = backupTaskTimeoutSeconds
	task.Steps = make([]testtaskjournal.TaskStepRecord, len(pointIDs))
	for index := range pointIDs {
		task.Steps[index] = testtaskjournal.TaskStepRecord{
			Kind: testtaskjournal.TaskStepOperation,
			ID:   ids.NewAt(ids.KindStep, now, int64(801+index)),
		}
	}
	task.Status = testtaskjournal.TaskStatusPending
	task.StartedAt = nil
	task.UpdatedAt = task.CreatedAt
	value, err := EncodeTaskRecord(task)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	created, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: testtaskjournal.TaskStorageKey(task.ID)}},
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(task.ID), Value: value},
		},
	)
	if err != nil || !created.Succeeded {
		t.Fatalf("seed checkpoint Task = %#v, %v", created, err)
	}
	agentID := ids.NewAt(ids.KindAgent, now, 802)
	assignment := testtaskassignments.TaskAssignmentRecord{
		AssignmentID: ids.NewAt(ids.KindAssignment, now, 803), TaskID: task.ID,
		Executor: testtaskjournal.TaskExecutorAgent, AgentID: agentID, AgentGeneration: 7,
		ClaimedTaskRevision: created.Revision, AssignedAt: now.Add(time.Second),
		Deadline: now.Add(6*time.Hour + time.Second), RecoveryDeadline: now.Add(12*time.Hour + time.Second),
		ExecutionMode: testtaskassignments.TaskExecutionModeForward, ExecutionEpoch: 1,
	}
	running := task
	running.Status = testtaskjournal.TaskStatusRunning
	running.StartedAt = &assignment.AssignedAt
	running.UpdatedAt = assignment.AssignedAt
	runningValue, err := EncodeTaskRecord(running)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(runningValue)
	assignmentValue, err := testtaskassignments.EncodeTaskAssignment(assignment)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(assignmentValue)
	claimed, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{
			{Key: testtaskjournal.TaskStorageKey(task.ID), ModRevision: created.Revision},
			{Key: testtaskjournal.TaskAssignmentKey(agentID, task.ID)},
			{Key: testtaskjournal.TaskAssignmentIndexKey(task.ID)},
			{Key: testtaskjournal.TaskTimeoutIndexKey(task.ID, assignment.Deadline)},
		},
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(task.ID), Value: runningValue},
			{
				Type:  testkeyvalue.MutationPut,
				Key:   testtaskjournal.TaskAssignmentKey(agentID, task.ID),
				Value: assignmentValue,
			},
			{
				Type:  testkeyvalue.MutationPut,
				Key:   testtaskjournal.TaskAssignmentIndexKey(task.ID),
				Value: assignmentValue,
			},
			{
				Type:  testkeyvalue.MutationPut,
				Key:   testtaskjournal.TaskTimeoutIndexKey(task.ID, assignment.Deadline),
				Value: assignmentValue,
			},
		},
	)
	if err != nil || !claimed.Succeeded {
		t.Fatalf("seed checkpoint assignment = %#v, %v", claimed, err)
	}
	return testbackupruntime.BackupCheckpointInput{
		TaskID: task.ID, AssignmentID: assignment.AssignmentID, AgentID: agentID,
		AgentGeneration: assignment.AgentGeneration, StepID: task.Steps[0].ID, Sequence: 1,
		Payload: testbackupruntime.BackupCheckpointPayload{
			Kind: testbackupruntime.BackupCheckpointArtifactPrepared, PointID: pointIDs[0],
			StoredSizeBytes: 123, StoredSHA256: testBackupDigest,
		},
	}, claimed.Revision
}
