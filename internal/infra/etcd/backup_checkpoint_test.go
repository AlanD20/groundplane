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
	"google.golang.org/protobuf/proto"
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
	changed.Request = proto.CloneOf(input.Request)
	changed.Request.GetArtifactPrepared().Evidence.StoredSha256[0] ^= 1
	if _, err := repository.loadBackupCheckpointPlan(
		context.Background(), changed, result.Revision, binding,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("loadBackupCheckpointPlan(changed digest) error = %v", err)
	}
	next := input
	next.Sequence++
	next.Request = proto.CloneOf(input.Request)
	next.Request.CheckpointSequence = next.Sequence
	next.PrecedingCheckpointRevision = result.Revision
	next.Request.PrecedingCheckpoint = &agentpb.CheckpointFence{
		DedupeKeyModRevision: result.Revision, AuthorityDigest: append([]byte(nil), input.Request.AuthorityDigest...),
	}
	next.Request.Checkpoint = &agentpb.BackupCheckpointRequest_SourceCleanupCompleted{
		SourceCleanupCompleted: &agentpb.BackupSourceCleanupCompleted{
			PointId: binding.pointID, Evidence: proto.CloneOf(input.Request.GetArtifactPrepared().Evidence),
		},
	}
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
	if replay, err := repository.loadBackupCheckpointPlan(
		context.Background(), input, nextResult.Revision, binding,
	); err != nil || !replay.duplicate {
		t.Fatalf("loadBackupCheckpointPlan(retained old sequence) = %#v, %v", replay, err)
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
		input.Request.GetArtifactPrepared().PointId = run.Sources[1].RecoveryPointID
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
		input.Request.Checkpoint = &agentpb.BackupCheckpointRequest_PruneObjectDeleted{
			PruneObjectDeleted: &agentpb.BackupPruneObjectDeleted{
				Ordinal: 2, PointId: pointIDs[1], Object: backupCheckpointPruneObject(run, pointIDs[1]),
			},
		}
		if _, err := repository.loadBackupCheckpointPlan(
			context.Background(), input, revision,
			backupCheckpointBinding{taskType: testtaskjournal.TaskBackupPrune, ordinal: 1, pointID: pointIDs[1]},
		); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Fatalf("loadBackupCheckpointPlan(cross-prune step) error = %v", err)
		}
	})
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
	now := time.Now().UTC().Truncate(time.Millisecond)
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
	task.TimeoutSeconds = backupTaskTimeout(task.Type)
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
		BackupAuthorityFence: &testtaskassignments.BackupAuthorityFence{
			AssignmentGeneration: 1, AuthoritySHA256: testBackupDigest,
		},
	}
	for _, step := range task.Steps {
		assignment.BackupAuthorityFence.Steps = append(assignment.BackupAuthorityFence.Steps,
			testtaskassignments.BackupStepAuthorityFence{
				StepID: step.ID, ExecutionID: ids.NewULID(), AuthoritySHA256: testBackupDigest,
			})
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
	digest, err := hex.DecodeString(testBackupDigest)
	if err != nil {
		t.Fatal(err)
	}
	sameInode := false
	request := &agentpb.BackupCheckpointRequest{
		TaskId: task.ID, AssignmentId: assignment.AssignmentID, StepId: task.Steps[0].ID,
		ExecutionId:        assignment.BackupAuthorityFence.Steps[0].ExecutionID,
		CheckpointSequence: 1, AuthorityDigest: digest,
		Checkpoint: &agentpb.BackupCheckpointRequest_ArtifactPrepared{ArtifactPrepared: &agentpb.BackupArtifactPrepared{
			PointId: pointIDs[0],
			Evidence: &agentpb.BackupArtifactEvidence{
				SourceSizeBytes: 4096, SourceSha256: append([]byte(nil), digest...),
				StoredSizeBytes: 4296, StoredSha256: append([]byte(nil), digest...),
			},
			Finals: &agentpb.BackupStagingFinals{SameInode: &sameInode,
				SourceRelativeName: executionplan.BackupSourceStagingFinal, StoredRelativeName: executionplan.BackupStoredStagingFinal},
			Archive: &agentpb.BackupArtifactPrepared_Postgres{
				Postgres: &agentpb.BackupPostgresArchiveEvidence{PgDumpMajor: 16, AdapterContractVersion: 1,
					SourceServerVersion: "16.9", BackupToolVersion: "16.9"},
			},
		}},
	}
	return testbackupruntime.BackupCheckpointInput{
		TaskID: task.ID, AssignmentID: assignment.AssignmentID, AgentID: agentID,
		AgentGeneration: assignment.AgentGeneration, StepID: task.Steps[0].ID, Sequence: 1,
		ExecutionID: request.ExecutionId, AuthoritySHA256: testBackupDigest,
		AssignmentGeneration: 1, Request: request,
	}, claimed.Revision
}

func backupCheckpointPruneObject(run testbackupruntime.BackupRunRecord, pointID string) *agentpb.BackupObjectIdentity {
	pathStyle := run.ConnectorPathStyle
	revision := func(value int64) *agentpb.RevisionDigest {
		return &agentpb.RevisionDigest{ModRevision: value, Sha256: bytes.Repeat([]byte{1}, 32)}
	}
	return &agentpb.BackupObjectIdentity{
		Connector: &agentpb.BackupConnectorAuthority{
			ConnectorId: run.ConnectorID, Connector: revision(run.ConnectorRevision),
			CanonicalEndpointUrl: run.ConnectorEndpoint, Region: run.ConnectorRegion, Prefix: run.ConnectorPrefix,
			PathStyle: &pathStyle, AccessKeySlotId: "access", SecretKeySlotId: "secret",
			AccessKeySlot: revision(1), SecretKeySlot: revision(1),
		},
		Bucket:    run.ConnectorBucket,
		ObjectKey: run.ConnectorPrefix + run.EnvironmentID + "/" + run.Sources[0].SourceID + "/" + pointID + "/artifact.bin",
		Discriminator: &agentpb.BackupObjectIdentity_VersionId{
			VersionId: &agentpb.BackupS3VersionId{Value: "version-1"},
		},
	}
}
