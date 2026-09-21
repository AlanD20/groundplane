package agent

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
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
// lifecycle conflict, rather than strand it or classify cancellation as an
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
			} else if err := pool.Abort(context.Background(), assignment.TaskID, assignment.AssignmentID); err != nil {
				t.Fatalf("Abort() error = %v", err)
			}
			close(consumeContext.proceed)
			if err := <-errCh; !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
				t.Fatalf("ConsumeBackupSecretSlot() error = %v, want state conflict", err)
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
	return nil
}

func agentBackupSecretAssignment(t *testing.T) testtaskassignment.Assignment {
	t.Helper()
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP,
		TargetId:  "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Steps: []*agentpb.ExecutionStep{{
			StepId: workerTestStepID, TimeoutSeconds: 60,
			Payload: &agentpb.ExecutionStep_BackupSourceCapture{BackupSourceCapture: &agentpb.BackupSourceCapture{
				SourceId: "spt_01ARZ3NDEKTSV4RRFFQ69G5FAV", SourceRevision: 2,
				TargetId: "att_01ARZ3NDEKTSV4RRFFQ69G5FAV", TargetRevision: 3,
				PointId: "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV", ConnectorId: "con_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				ConnectorRevision: 4,
				SourceFormat:      agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_POSTGRES_CUSTOM_V1,
				Encryption:        agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE,
				Upload: &agentpb.BackupUploadAuthority{
					ConnectorEndpoint: "https://objects.example.test", ConnectorBucket: "groundplane-backups",
					ConnectorPrefix: "production/", ConnectorRegion: "auto",
					ConnectorAddressing: agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_PATH_STYLE,
					ProtectedObjectKey: "production/env_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
						"spt_01ARZ3NDEKTSV4RRFFQ69G5FAV/rp_01ARZ3NDEKTSV4RRFFQ69G5FAV/artifact.bin",
					ImmutableCreate: true, PutAfterArtifactPreparedAck: true, HeadAfterUploadCompletedAck: true,
				},
				Source: &agentpb.BackupSourceCapture_Attach{Attach: &agentpb.BackupAttachSource{
					BackingServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", BackingServiceRevision: 5,
					Database: "application", Role: "application_owner",
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Seal(Backup) error = %v", err)
	}
	return testtaskassignment.Assignment{
		AssignmentID: workerTestAssignmentID, TaskID: workerTestTaskID,
		OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV", Plan: plan,
		ExecutionEpoch: 1, ExecutionMode: agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		ForwardDeadline: time.Now().Add(120 * time.Second), RecoveryDeadline: time.Now().Add(240 * time.Second),
	}
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
