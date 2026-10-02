package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// QA: CON-07; in-memory slot assembly and zeroization only, not Controller resolution or subprocess exposure.
// Rationale: the inbox must assemble only the exact assignment slot, transfer
// ownership to one callback, and clear its buffer immediately after consume.
func TestClientClearsBackupSecretTransferChunk(t *testing.T) {
	pool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
	assignment := agentBackupSecretAssignment(t)
	if err := pool.backupSecrets.Register(assignment); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	client := &Client{pool: pool}
	frames := agentBackupSecretFrames(assignment, []byte("access"))
	if _, err := client.handleControllerMessage(context.Background(), &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_BackupSecretSlotTransfer{BackupSecretSlotTransfer: frames[0]},
	}); err != nil {
		t.Fatalf("handleControllerMessage(header) error = %v", err)
	}
	chunkAlias := frames[1].GetChunk().Content
	if _, err := client.handleControllerMessage(context.Background(), &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_BackupSecretSlotTransfer{BackupSecretSlotTransfer: frames[1]},
	}); err != nil {
		t.Fatalf("handleControllerMessage(chunk) error = %v", err)
	}
	if frames[1].GetChunk().Content != nil || !agentAllZero(chunkAlias) {
		t.Fatal("Client retained received Backup secret chunk")
	}
	if _, err := client.handleControllerMessage(context.Background(), &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_BackupSecretSlotTransfer{BackupSecretSlotTransfer: frames[2]},
	}); err != nil {
		t.Fatalf("handleControllerMessage(end) error = %v", err)
	}
	if err := pool.ConsumeBackupSecretSlot(
		context.Background(), assignment.TaskID, assignment.AssignmentID, workerTestStepID,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		func(content []byte) error {
			if !bytes.Equal(content, []byte("access")) {
				t.Fatalf("inbox copy = %q, want access", content)
			}
			return nil
		},
	); err != nil {
		t.Fatalf("ConsumeBackupSecretSlot() error = %v", err)
	}
}

// QA: CON-07, TASK-10; in-memory assignment fence only, not authenticated generation or Controller authorization.
// Rationale: stale assignment traffic must not mutate the currently reserved
// slot or close its ready signal.
func TestWorkerPoolClearsBackupSecretsBeforeTerminalOutput(t *testing.T) {
	pool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
	assignment := agentBackupSecretAssignment(t)
	if err := pool.Submit(context.Background(), assignment); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	for _, frame := range agentBackupSecretFrames(assignment, []byte("access")) {
		if err := pool.AcceptBackupSecretSlotTransfer(context.Background(), frame); err != nil {
			t.Fatalf("AcceptBackupSecretSlotTransfer() error = %v", err)
		}
	}
	pool.mu.Lock()
	reservation := pool.reservations[assignment.TaskID]
	pool.mu.Unlock()
	if reservation == nil {
		t.Fatal("reservation = nil")
	}
	done := make(chan struct{})
	go func() {
		pool.complete(context.Background(), reservation, TaskResult{
			AssignmentID: assignment.AssignmentID,
			TaskID:       assignment.TaskID,
			PlanHash:     testtaskassignment.PlanDigest(assignment.Plan),
			Terminal:     TaskTerminalFailed,
		})
		close(done)
	}()
	output := <-pool.Outputs()
	if output.Result == nil {
		t.Fatal("terminal WorkerOutput result = nil")
	}
	if err := pool.ConsumeBackupSecretSlot(
		context.Background(), assignment.TaskID, assignment.AssignmentID, workerTestStepID,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		func([]byte) error { return nil },
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("terminal WorkerOutput preceded Backup secret release: %v", err)
	}
	<-done
}

// QA: CON-07, BAK-16; controlled in-memory race only, not live stream loss or external consumer termination.
// Rationale: abort and stream teardown must wake a blocked consumer with a
// lifecycle cancellation/conflict, rather than strand it or classify cancellation as an
// internal corruption.
func TestWorkerPoolBlockedBackupSecretConsumeRacingAbortAndStop(t *testing.T) {
	for _, stop := range []bool{false, true} {
		name := "abort"
		if stop {
			name = "stop"
		}
		t.Run(name, func(t *testing.T) {
			pool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
			assignment := agentBackupSecretAssignment(t)
			if err := pool.Submit(context.Background(), assignment); err != nil {
				t.Fatalf("Submit() error = %v", err)
			}
			frames := agentBackupSecretFrames(assignment, []byte("access"))
			for _, frame := range frames[:2] {
				if err := pool.AcceptBackupSecretSlotTransfer(context.Background(), frame); err != nil {
					t.Fatalf("AcceptBackupSecretSlotTransfer() error = %v", err)
				}
			}
			consumeContext := newObservedDoneContext()
			pool.mu.Lock()
			consumeContext.Context = pool.reservations[assignment.TaskID].ctx
			pool.mu.Unlock()
			errCh := make(chan error, 1)
			go func() {
				errCh <- pool.ConsumeBackupSecretSlot(
					consumeContext, assignment.TaskID, assignment.AssignmentID, workerTestStepID,
					agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
					func([]byte) error { return nil },
				)
			}()
			<-consumeContext.observed
			if stop {
				pool.stop()
			} else if err := pool.AbortMessage(context.Background(), &agentpb.TaskAbort{
				TaskId: assignment.TaskID, AssignmentId: assignment.AssignmentID,
				AssignmentGeneration: assignment.AssignmentGeneration, PlanHash: assignment.Plan.PlanHash,
			}); err != nil {
				t.Fatalf("Abort() error = %v", err)
			}
			close(consumeContext.proceed)
			if err := <-errCh; !errors.Is(err, context.Canceled) &&
				!(stop && errors.Is(err, errs.New(errs.KindStateConflict, ""))) {
				t.Fatalf("ConsumeBackupSecretSlot() error = %v, want lifecycle cancellation", err)
			}
		})
	}
}

// QA: CON-07; controlled inbox release/consume race only, not worker or external process cancellation.
// Rationale: a complete slot can be selected by Consume while lifecycle
// release wins the following lock. That cancellation is a state conflict, not
// an impossible internal state.
func TestWorkerPoolBackupSecretInboxResetsAcrossReconnectRedispatch(t *testing.T) {
	assignment := agentBackupSecretAssignment(t)
	oldPool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
	if err := oldPool.Submit(context.Background(), assignment); err != nil {
		t.Fatalf("old Submit() error = %v", err)
	}
	for _, frame := range agentBackupSecretFrames(assignment, []byte("old-access")) {
		if err := oldPool.AcceptBackupSecretSlotTransfer(context.Background(), frame); err != nil {
			t.Fatalf("old AcceptBackupSecretSlotTransfer() error = %v", err)
		}
	}
	oldPool.stop()
	if err := oldPool.ConsumeBackupSecretSlot(
		context.Background(), assignment.TaskID, assignment.AssignmentID, workerTestStepID,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		func([]byte) error { return nil },
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("stream teardown retained old slot authority: %v", err)
	}

	newPool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
	if err := newPool.Submit(context.Background(), assignment); err != nil {
		t.Fatalf("redispatch Submit() error = %v", err)
	}
	for _, frame := range agentBackupSecretFrames(assignment, []byte("fresh-access")) {
		if err := newPool.AcceptBackupSecretSlotTransfer(context.Background(), frame); err != nil {
			t.Fatalf("new AcceptBackupSecretSlotTransfer() error = %v", err)
		}
	}
	if err := newPool.ConsumeBackupSecretSlot(
		context.Background(), assignment.TaskID, assignment.AssignmentID, workerTestStepID,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		func(content []byte) error {
			if !bytes.Equal(content, []byte("fresh-access")) {
				t.Fatalf("redispatched content = %q", content)
			}
			return nil
		},
	); err != nil {
		t.Fatalf("ConsumeBackupSecretSlot() error = %v", err)
	}
	newPool.stop()
}

type observedDoneContext struct {
	context.Context
	observed chan struct{}
	proceed  chan struct{}
	once     sync.Once
}

func newObservedDoneContext() *observedDoneContext {
	return &observedDoneContext{
		Context:  context.Background(),
		observed: make(chan struct{}),
		proceed:  make(chan struct{}),
	}
}

func (ctx *observedDoneContext) Done() <-chan struct{} {
	ctx.once.Do(func() {
		close(ctx.observed)
		<-ctx.proceed
	})
	return ctx.Context.Done()
}

func agentBackupSecretAssignment(t *testing.T) testtaskassignment.Assignment {
	t.Helper()
	pathStyle := true
	forwardDeadline := time.Now().Add(120 * time.Second)
	digest := bytes.Repeat([]byte{1}, sha256.Size)
	stepAuthority, err := executionplan.SealBackupStepAuthority(&agentpb.BackupStepAuthority{
		StepId: workerTestStepID, ExecutionId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		StepDeadlineUnixNano: uint64(forwardDeadline.UnixNano()),
		Operation: &agentpb.BackupStepAuthority_Prune{Prune: &agentpb.BackupPruneAuthority{
			RetentionPolicy: agentBackupSecretRevision(1, 1), Objects: []*agentpb.BackupPruneObject{{
				Ordinal: 1, PointId: "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Point: agentBackupSecretRevision(2, 2),
				Evidence: &agentpb.BackupArtifactEvidence{
					SourceSizeBytes: 4096, SourceSha256: append([]byte(nil), digest...),
					StoredSizeBytes: 4096, StoredSha256: append([]byte(nil), digest...),
				},
				Object: &agentpb.BackupObjectIdentity{
					Connector: &agentpb.BackupConnectorAuthority{
						ConnectorId: "con_01ARZ3NDEKTSV4RRFFQ69G5FAV", Connector: agentBackupSecretRevision(3, 3),
						CanonicalEndpointUrl: "https://objects.example.test", Region: "auto", PathStyle: &pathStyle,
						Prefix: "production/", AccessKeySlotId: backupsecret.AccessKeySlotID,
						SecretKeySlotId: backupsecret.SecretKeySlotID,
						AccessKeySlot: agentBackupSecretRevision(
							4,
							4,
						), SecretKeySlot: agentBackupSecretRevision(5, 5),
					},
					Bucket: "groundplane-backups",
					ObjectKey: "production/env_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
						"spt_01ARZ3NDEKTSV4RRFFQ69G5FAV/rp_01ARZ3NDEKTSV4RRFFQ69G5FAV/artifact.bin",
					Discriminator: &agentpb.BackupObjectIdentity_Etag{Etag: &agentpb.BackupS3ETag{Value: "etag"}},
				},
				MetadataCount: 10, MetadataSha256: bytes.Repeat([]byte{6}, sha256.Size),
			}},
		}},
	})
	if err != nil {
		t.Fatalf("SealBackupStepAuthority() error = %v", err)
	}
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE,
		TargetId:  "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		BackupScope: &agentpb.BackupPlanScope{
			ProjectId: "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV", Project: agentBackupSecretRevision(6, 6),
			EnvironmentId: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", Environment: agentBackupSecretRevision(7, 7),
			TaskAttempt: 1,
		},
		Steps: []*agentpb.ExecutionStep{{
			StepId: workerTestStepID, TimeoutSeconds: executionplan.MaximumBackupPruneStepTimeoutSeconds,
			Payload: &agentpb.ExecutionStep_BackupStep{BackupStep: stepAuthority},
		}},
	})
	if err != nil {
		t.Fatalf("Seal(Backup) error = %v", err)
	}
	authority, authorityDigest, err := executionplan.BindBackupTaskAuthority(
		plan,
		executionplan.BackupAssignmentIdentity{
			TaskID: workerTestTaskID, OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			AssignmentID: workerTestAssignmentID, Generation: 1, DeadlineUnixNano: uint64(forwardDeadline.UnixNano()),
		},
	)
	if err != nil {
		t.Fatalf("BindBackupTaskAuthority() error = %v", err)
	}
	resume := &agentpb.BackupTaskResume{
		AssignmentId: workerTestAssignmentID, AssignmentGeneration: 1,
		Steps: []*agentpb.BackupStepResume{{
			StepId: stepAuthority.StepId, ExecutionId: stepAuthority.ExecutionId,
			Operation: &agentpb.BackupStepResume_Prune{Prune: &agentpb.BackupPruneResume{NextObjectOrdinal: 1}},
		}},
	}
	return testtaskassignment.Assignment{
		BackupAuthority: authority, BackupResume: resume,
		BackupAuthoritySHA256: authorityDigest, AssignmentGeneration: 1,
		AssignmentID: workerTestAssignmentID, TaskID: workerTestTaskID,
		OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV", Plan: plan,
		ExecutionEpoch: 1, ExecutionMode: agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		ForwardDeadline: forwardDeadline, RecoveryDeadline: forwardDeadline.Add(120 * time.Second),
	}
}

func agentBackupSecretRevision(revision int64, fill byte) *agentpb.RevisionDigest {
	return &agentpb.RevisionDigest{ModRevision: revision, Sha256: bytes.Repeat([]byte{fill}, sha256.Size)}
}

func agentBackupSecretFrames(
	assignment testtaskassignment.Assignment,
	content []byte,
) []*agentpb.BackupSecretSlotTransfer {
	purpose := agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY
	identity := func() *agentpb.BackupSecretSlotTransfer {
		return &agentpb.BackupSecretSlotTransfer{
			TaskId: assignment.TaskID, AssignmentId: assignment.AssignmentID,
			StepId: workerTestStepID, Purpose: purpose,
		}
	}
	header := identity()
	header.Record = &agentpb.BackupSecretSlotTransfer_Header{
		Header: &agentpb.BackupSecretSlotHeader{TotalBytes: uint64(len(content)), ChunkCount: 1},
	}
	chunk := identity()
	chunk.Record = &agentpb.BackupSecretSlotTransfer_Chunk{
		Chunk: &agentpb.BackupSecretSlotChunk{Sequence: 1, Content: append([]byte(nil), content...)},
	}
	end := identity()
	end.Record = &agentpb.BackupSecretSlotTransfer_End{End: &agentpb.BackupSecretSlotEnd{ChunkCount: 1}}
	return []*agentpb.BackupSecretSlotTransfer{header, chunk, end}
}

func agentAllZero(value []byte) bool {
	for _, current := range value {
		if current != 0 {
			return false
		}
	}
	return true
}
