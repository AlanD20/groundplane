package backupsecrettransfer

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

const (
	backupSecretTestAssignmentID = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	backupSecretTestTaskID       = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	backupSecretTestStepID       = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func TestBackupSecretInboxConsumesOnceAndClears(t *testing.T) {
	inbox := New()
	assignment := agentBackupSecretAssignment(t)
	if err := inbox.Register(assignment); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	frames := agentBackupSecretFrames(assignment, []byte("access"))
	for _, frame := range frames {
		if err := inbox.Accept(context.Background(), frame); err != nil {
			t.Fatalf("Accept() error = %v", err)
		}
	}
	slot := inbox.tasks[assignment.TaskID].steps[backupSecretTestStepID][agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY]
	alias := slot.content
	if err := inbox.Consume(
		context.Background(), assignment.TaskID, assignment.AssignmentID, backupSecretTestStepID,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		func(content []byte) error {
			if !bytes.Equal(content, []byte("access")) {
				t.Fatalf("consumed content = %q", content)
			}
			return nil
		},
	); err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !agentAllZero(alias) {
		t.Fatal("Consume() retained slot bytes")
	}
	if err := inbox.Consume(
		context.Background(), assignment.TaskID, assignment.AssignmentID, backupSecretTestStepID,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		func([]byte) error { return nil },
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Consume(second) error = %v, want state conflict", err)
	}
}

// QA: CON-07; local Agent receive ownership only, not gRPC memory reclamation or durable secret absence.
// Rationale: the gRPC receive object is untrusted transient ownership; the
// Client must clear it after the inbox has copied the accepted chunk.
func TestBackupSecretInboxRejectsStaleAssignment(t *testing.T) {
	inbox := New()
	assignment := agentBackupSecretAssignment(t)
	if err := inbox.Register(assignment); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	frame := agentBackupSecretFrames(assignment, []byte("access"))[0]
	frame.AssignmentId = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if err := inbox.Accept(context.Background(), frame); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Accept(stale assignment) error = %v, want state conflict", err)
	}
	for _, valid := range agentBackupSecretFrames(assignment, []byte("valid")) {
		if err := inbox.Accept(context.Background(), valid); err != nil {
			t.Fatalf("Accept(valid after stale) error = %v", err)
		}
	}
	if err := inbox.Consume(
		context.Background(), assignment.TaskID, assignment.AssignmentID, backupSecretTestStepID,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		func(content []byte) error {
			if !bytes.Equal(content, []byte("valid")) {
				t.Fatalf("content after stale frame = %q, want valid", content)
			}
			return nil
		},
	); err != nil {
		t.Fatalf("Consume(valid after stale) error = %v", err)
	}
}

// QA: CON-07, BAK-05/08; local plan-to-slot selection only, not credential resolution or S3 execution.
// Rationale: capture encrypts with a public recipient and prune does not
// decrypt, so neither current operation may reserve or accept either private
// restore identity purpose.
func TestBackupSecretInboxReservesOnlyS3PurposesForCaptureAndPrune(t *testing.T) {
	for _, stepKind := range []string{"age capture", "prune"} {
		t.Run(stepKind, func(t *testing.T) {
			inbox := New()
			assignment := agentBackupSecretAssignment(t)
			if stepKind == "age capture" {
				assignment.Plan.GetSteps()[0].GetBackupSourceCapture().Encryption =
					agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE
			} else {
				assignment.Plan.GetSteps()[0].Payload = &agentpb.ExecutionStep_BackupArtifactPrune{
					BackupArtifactPrune: &agentpb.BackupArtifactPrune{},
				}
			}
			if err := inbox.Register(assignment); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			slots := inbox.tasks[assignment.TaskID].steps[backupSecretTestStepID]
			if len(slots) != 2 ||
				slots[agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY] == nil ||
				slots[agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY] == nil {
				t.Fatalf("reserved slots = %#v, want exact S3 pair", slots)
			}
			for _, purpose := range []agentpb.BackupSecretSlotPurpose{
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY,
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_OPERATOR_OLD_AGE_IDENTITY,
			} {
				frame := agentBackupSecretFrames(assignment, []byte("identity"))[0]
				frame.Purpose = purpose
				if err := inbox.Accept(context.Background(), frame); !errors.Is(
					err, errs.New(errs.KindStateConflict, ""),
				) {
					t.Fatalf("Accept(%s) error = %v, want state conflict", purpose, err)
				}
			}
		})
	}
}

// QA: CON-07, BAK-16; in-memory abort/stop zeroization only, not process memory or reconnect recovery.
// Rationale: abort and stream teardown share reservation ownership and must
// synchronously zero every partially or fully received slot.
func TestBackupSecretInboxCompleteReleaseRacingConsumeIsStateConflict(t *testing.T) {
	inbox := New()
	assignment := agentBackupSecretAssignment(t)
	if err := inbox.Register(assignment); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	for _, frame := range agentBackupSecretFrames(assignment, []byte("access")) {
		if err := inbox.Accept(context.Background(), frame); err != nil {
			t.Fatalf("Accept() error = %v", err)
		}
	}
	alias := inbox.tasks[assignment.TaskID].steps[backupSecretTestStepID][agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY].content
	consumeContext := newObservedDoneContext()
	errCh := make(chan error, 1)
	go func() {
		errCh <- inbox.Consume(
			consumeContext, assignment.TaskID, assignment.AssignmentID, backupSecretTestStepID,
			agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
			func([]byte) error { return nil },
		)
	}()
	<-consumeContext.observed
	inbox.Release(assignment.TaskID)
	close(consumeContext.proceed)
	if err := <-errCh; !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Consume() error = %v, want state conflict", err)
	}
	if !agentAllZero(alias) {
		t.Fatal("release retained complete slot bytes")
	}
}

// QA: CON-07, TASK-10; local transfer-state validation only, not wire truncation or durable credential absence.
// Rationale: every record transition is closed. Malformed, duplicate, extra,
// and incomplete streams fail closed and release any accumulated plaintext.
func TestBackupSecretInboxRejectsInvalidFrameSequences(t *testing.T) {
	t.Run("malformed header", func(t *testing.T) {
		inbox, assignment := registeredBackupSecretInbox(t)
		frame := agentBackupSecretFrames(assignment, []byte("access"))[0]
		frame.GetHeader().TotalBytes = 0
		if err := inbox.Accept(context.Background(), frame); !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("Accept(malformed header) error = %v, want internal", err)
		}
	})

	t.Run("duplicate header", func(t *testing.T) {
		inbox, assignment := registeredBackupSecretInbox(t)
		frames := agentBackupSecretFrames(assignment, []byte("access"))
		for _, frame := range frames[:2] {
			if err := inbox.Accept(context.Background(), frame); err != nil {
				t.Fatalf("Accept(initial record) error = %v", err)
			}
		}
		alias := inbox.tasks[assignment.TaskID].steps[backupSecretTestStepID][agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY].content
		if err := inbox.Accept(context.Background(), frames[0]); !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("Accept(duplicate header) error = %v, want internal", err)
		}
		if !agentAllZero(alias) {
			t.Fatal("duplicate header retained accumulated slot bytes")
		}
	})

	t.Run("extra after end", func(t *testing.T) {
		inbox, assignment := registeredBackupSecretInbox(t)
		frames := agentBackupSecretFrames(assignment, []byte("access"))
		for _, frame := range frames {
			if err := inbox.Accept(context.Background(), frame); err != nil {
				t.Fatalf("Accept() error = %v", err)
			}
		}
		alias := inbox.tasks[assignment.TaskID].steps[backupSecretTestStepID][agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY].content
		if err := inbox.Accept(context.Background(), frames[2]); !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("Accept(extra end) error = %v, want internal", err)
		}
		if !agentAllZero(alias) {
			t.Fatal("extra frame failure retained slot bytes")
		}
	})

	t.Run("missing end", func(t *testing.T) {
		inbox, assignment := registeredBackupSecretInbox(t)
		frames := agentBackupSecretFrames(assignment, []byte("access"))
		for _, frame := range frames[:2] {
			if err := inbox.Accept(context.Background(), frame); err != nil {
				t.Fatalf("Accept() error = %v", err)
			}
		}
		alias := inbox.tasks[assignment.TaskID].steps[backupSecretTestStepID][agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY].content
		inbox.Release(assignment.TaskID)
		if !agentAllZero(alias) {
			t.Fatal("missing end release retained slot bytes")
		}
	})
}

// QA: CON-07, BAK-16; recreated in-memory pools only, not actual reconnect, durable claim, or S3 effects.
// Rationale: stream loss destroys the old inbox. Redispatch of the identical
// durable claim on a fresh connection accepts only freshly transferred bytes.
func registeredBackupSecretInbox(t *testing.T) (*Inbox, testtaskassignment.Assignment) {
	t.Helper()
	inbox := New()
	assignment := agentBackupSecretAssignment(t)
	if err := inbox.Register(assignment); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	return inbox, assignment
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
			StepId: backupSecretTestStepID, TimeoutSeconds: 60,
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
		AssignmentID: backupSecretTestAssignmentID, TaskID: backupSecretTestTaskID,
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
			StepId: backupSecretTestStepID, Purpose: purpose,
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
