package backupsecrettransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	backupSecretTestTaskID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	backupSecretTestStepID = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func TestBackupSecretInboxConsumesOnceAndClears(t *testing.T) {
	inbox := NewWorkInbox()
	assignment := agentBackupSecretAssignment(t)
	if err := inbox.Register(assignment, backupSecretTestIdentity()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	frames := agentBackupSecretFrames(assignment, []byte("access"))
	for _, frame := range frames {
		if err := inbox.Accept(context.Background(), frame); err != nil {
			t.Fatalf("Accept() error = %v", err)
		}
	}
	slot := inbox.attempts[assignment.Attempt.AttemptId].slots[0][agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY]
	alias := slot.content
	if err := inbox.Consume(
		context.Background(), assignment.Attempt, 0,
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
		context.Background(), assignment.Attempt, 0,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		func([]byte) error { return nil },
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Consume(second) error = %v, want state conflict", err)
	}
}

// QA: CON-07; local Agent receive ownership only, not gRPC memory reclamation or durable secret absence.
// Rationale: a stale attempt must not populate or poison slots owned by
// the current attempt, which must still accept freshly transferred bytes.
func TestBackupSecretInboxRejectsStaleAssignment(t *testing.T) {
	inbox := NewWorkInbox()
	assignment := agentBackupSecretAssignment(t)
	if err := inbox.Register(assignment, backupSecretTestIdentity()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	frame := agentBackupSecretFrames(assignment, []byte("access"))[0]
	frame.Attempt.AttemptId = "try_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if err := inbox.Accept(context.Background(), frame); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Accept(stale assignment) error = %v, want state conflict", err)
	}
	for _, valid := range agentBackupSecretFrames(assignment, []byte("valid")) {
		if err := inbox.Accept(context.Background(), valid); err != nil {
			t.Fatalf("Accept(valid after stale) error = %v", err)
		}
	}
	if err := inbox.Consume(
		context.Background(), assignment.Attempt, 0,
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
// Rationale: capture may receive only the current Environment identity selected
// by its sealed authority, while prune receives no decryption identity.
func TestBackupSecretInboxReservesSealedPurposesForCaptureAndPrune(t *testing.T) {
	for _, stepKind := range []string{"age capture", "prune"} {
		t.Run(stepKind, func(t *testing.T) {
			inbox := NewWorkInbox()
			assignment := agentBackupSecretAssignment(t)
			if stepKind == "age capture" {
				assignment = agentBackupSecretCaptureAssignment(t)
			}
			if err := inbox.Register(assignment, backupSecretTestIdentity()); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			slots := inbox.attempts[assignment.Attempt.AttemptId].slots[0]
			wantSlots := 2
			if stepKind == "age capture" {
				wantSlots = 3
			}
			if len(slots) != wantSlots ||
				slots[agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY] == nil ||
				slots[agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY] == nil {
				t.Fatalf("reserved slots = %#v, want %d authority-selected slots", slots, wantSlots)
			}
			if stepKind == "age capture" &&
				slots[agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY] == nil {
				t.Fatal("age capture did not reserve its sealed current identity")
			}
			for _, purpose := range []agentpb.BackupSecretSlotPurpose{
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
	inbox := NewWorkInbox()
	assignment := agentBackupSecretAssignment(t)
	if err := inbox.Register(assignment, backupSecretTestIdentity()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	for _, frame := range agentBackupSecretFrames(assignment, []byte("access")) {
		if err := inbox.Accept(context.Background(), frame); err != nil {
			t.Fatalf("Accept() error = %v", err)
		}
	}
	alias := inbox.attempts[assignment.Attempt.AttemptId].slots[0][agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY].content
	consumeContext := newObservedDoneContext()
	errCh := make(chan error, 1)
	go func() {
		errCh <- inbox.Consume(
			consumeContext, assignment.Attempt, 0,
			agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
			func([]byte) error { return nil },
		)
	}()
	<-consumeContext.observed
	inbox.Release(assignment.Attempt.AttemptId)
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
		if err := inbox.Accept(context.Background(), frame); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			t.Fatalf("Accept(malformed header) error = %v, want state conflict", err)
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
		alias := inbox.attempts[assignment.Attempt.AttemptId].slots[0][agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY].content
		if err := inbox.Accept(context.Background(), frames[0]); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			t.Fatalf("Accept(duplicate header) error = %v, want state conflict", err)
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
		alias := inbox.attempts[assignment.Attempt.AttemptId].slots[0][agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY].content
		if err := inbox.Accept(context.Background(), frames[2]); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			t.Fatalf("Accept(extra end) error = %v, want state conflict", err)
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
		alias := inbox.attempts[assignment.Attempt.AttemptId].slots[0][agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY].content
		inbox.Release(assignment.Attempt.AttemptId)
		if !agentAllZero(alias) {
			t.Fatal("missing end release retained slot bytes")
		}
	})
}

// QA: CON-07, BAK-16; recreated in-memory pools only, not actual reconnect, durable claim, or S3 effects.
// Rationale: stream loss destroys the old inbox. Redispatch of the identical
// durable claim on a fresh connection accepts only freshly transferred bytes.
func registeredBackupSecretInbox(t *testing.T) (*WorkInbox, *agentpb.BackupWorkAssignment) {
	t.Helper()
	inbox := NewWorkInbox()
	assignment := agentBackupSecretAssignment(t)
	if err := inbox.Register(assignment, backupSecretTestIdentity()); err != nil {
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

func agentBackupSecretAssignment(t *testing.T) *agentpb.BackupWorkAssignment {
	t.Helper()
	pathStyle := true
	deadline := time.Now().Add(120 * time.Second)
	digest := bytes.Repeat([]byte{1}, sha256.Size)
	authority, err := executionplan.SealBackupStepAuthority(&agentpb.BackupStepAuthority{
		StepId: backupSecretTestStepID, ExecutionId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		StepDeadlineUnixNano: uint64(deadline.UnixNano()),
		Operation: &agentpb.BackupStepAuthority_Prune{Prune: &agentpb.BackupPruneAuthority{
			RetentionPolicy: backupSecretRevision(1, 1), Objects: []*agentpb.BackupPruneObject{{
				Ordinal: 1, PointId: "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Point: backupSecretRevision(2, 2),
				Evidence: &agentpb.BackupArtifactEvidence{
					SourceSizeBytes: 4096, SourceSha256: append([]byte(nil), digest...),
					StoredSizeBytes: 4096, StoredSha256: append([]byte(nil), digest...),
				},
				Object: &agentpb.BackupObjectIdentity{
					Connector: &agentpb.BackupConnectorAuthority{
						ConnectorId: "con_01ARZ3NDEKTSV4RRFFQ69G5FAV", Connector: backupSecretRevision(3, 3),
						CanonicalEndpointUrl: "https://objects.example.test", Region: "auto", PathStyle: &pathStyle,
						Prefix: "production/", AccessKeySlotId: backupsecret.AccessKeySlotID,
						SecretKeySlotId: backupsecret.SecretKeySlotID,
						AccessKeySlot:   backupSecretRevision(4, 4), SecretKeySlot: backupSecretRevision(5, 5),
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
	input := &agentpb.BackupWorkInput{
		Schema: 1, Operation: agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE,
		ProjectId: "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV", Project: backupSecretRevision(6, 6),
		EnvironmentId: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", Environment: backupSecretRevision(7, 7),
		Steps: []*agentpb.BackupStepAuthority{authority},
	}
	return backupSecretAssignmentForInput(t, input, deadline)
}

func backupSecretAssignmentForInput(
	t *testing.T,
	input *agentpb.BackupWorkInput,
	deadline time.Time,
) *agentpb.BackupWorkAssignment {
	t.Helper()
	owned, raw, reference, err := executionplan.CaptureBackupWorkInput(ids.New(ids.KindPlan), input)
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := workexecution.EncodeAgentAttempt(workexecution.Attempt{
		ID: ids.New(ids.KindAttempt), TaskID: backupSecretTestTaskID, WorkItemID: ids.New(ids.KindWorkItem), Number: 1,
		Executor: backupSecretTestIdentity(), Direction: workexecution.Forward, Input: reference,
		AssignedAt: time.Now().UTC(), Deadline: deadline.UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	source := &agentpb.BackupStepResume{StepId: input.Steps[0].StepId, ExecutionId: input.Steps[0].ExecutionId}
	if input.Operation == agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE {
		source.Operation = &agentpb.BackupStepResume_Prune{Prune: &agentpb.BackupPruneResume{NextObjectOrdinal: 1}}
	} else {
		source.Operation = &agentpb.BackupStepResume_Capture{Capture: &agentpb.BackupCaptureResume{
			Phase: agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING, Cursor: &agentpb.BackupCaptureCursor{ObjectAttempt: 1},
		}}
	}
	return &agentpb.BackupWorkAssignment{Attempt: attempt, Input: owned, Sources: []*agentpb.BackupStepResume{source}}
}

func agentBackupSecretCaptureAssignment(t *testing.T) *agentpb.BackupWorkAssignment {
	t.Helper()
	prune := agentBackupSecretAssignment(t)
	object := prune.Input.Steps[0].GetPrune().Objects[0].Object
	source, err := executionplan.SealBackupStepAuthority(&agentpb.BackupStepAuthority{
		StepId: backupSecretTestStepID, ExecutionId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		StepDeadlineUnixNano: uint64(prune.Attempt.Deadline.AsTime().UnixNano()),
		Operation: &agentpb.BackupStepAuthority_Capture{Capture: &agentpb.BackupCaptureAuthority{
			PointId: "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			Resource: &agentpb.BackupResourceIdentity{
				Kind:       agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ENVIRONMENT,
				ResourceId: prune.Input.EnvironmentId,
				Resource:   backupSecretRevision(7, 7),
			},
			Target: &agentpb.BackupObjectTarget{
				Connector: proto.CloneOf(object.Connector),
				Bucket:    object.Bucket,
				ObjectKey: object.ObjectKey,
			},
			Encryption: &agentpb.BackupEncryptionAuthority{
				Kind: agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE, SecretSlotId: backupsecret.CurrentAgeIdentitySlotID,
				RecipientSha256: bytes.Repeat(
					[]byte{8},
					sha256.Size,
				), SecretSlot: backupSecretRevision(8, 8), KeyEra: backupSecretUint64(1),
			},
			Source: &agentpb.BackupCaptureAuthority_Config{Config: &agentpb.BackupConfigCaptureAuthority{
				EnvironmentId: prune.Input.EnvironmentId, Content: &agentpb.BackupConfigContentAuthority{
					ManifestSha256: bytes.Repeat(
						[]byte{9},
						sha256.Size,
					), EntryCount: 1, TotalSelectedValueBytes: 32, ManifestSizeBytes: 128,
					SourceSizeBytes: 1536, MetadataSnapshotSha256: bytes.Repeat([]byte{10}, sha256.Size),
				}, MetadataSnapshotRevision: 8, MetadataEntryCount: 1, MetadataProtoBytes: 128,
			}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := proto.CloneOf(prune.Input)
	input.Operation = agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE
	input.Steps = []*agentpb.BackupStepAuthority{source}
	return backupSecretAssignmentForInput(t, input, prune.Attempt.Deadline.AsTime())
}

func backupSecretUint64(value uint64) *uint64 { return &value }

func backupSecretTestIdentity() workexecution.Identity {
	return workexecution.Identity{
		Executor:   workexecution.Agent,
		HostID:     "host_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		AgentID:    "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Generation: 1,
	}
}

func backupSecretRevision(revision int64, fill byte) *agentpb.RevisionDigest {
	return &agentpb.RevisionDigest{ModRevision: revision, Sha256: bytes.Repeat([]byte{fill}, sha256.Size)}
}

func agentBackupSecretFrames(
	assignment *agentpb.BackupWorkAssignment,
	content []byte,
) []*agentpb.BackupWorkSecretTransfer {
	purpose := agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY
	identity := func() *agentpb.BackupWorkSecretTransfer {
		return &agentpb.BackupWorkSecretTransfer{
			Attempt: proto.CloneOf(assignment.Attempt), Purpose: purpose,
		}
	}
	header := identity()
	header.Record = &agentpb.BackupWorkSecretTransfer_Header{
		Header: &agentpb.BackupSecretSlotHeader{TotalBytes: uint64(len(content)), ChunkCount: 1},
	}
	chunk := identity()
	chunk.Record = &agentpb.BackupWorkSecretTransfer_Chunk{
		Chunk: &agentpb.BackupSecretSlotChunk{Sequence: 1, Content: append([]byte(nil), content...)},
	}
	end := identity()
	end.Record = &agentpb.BackupWorkSecretTransfer_End{End: &agentpb.BackupSecretSlotEnd{ChunkCount: 1}}
	return []*agentpb.BackupWorkSecretTransfer{header, chunk, end}
}

func agentAllZero(value []byte) bool {
	for _, current := range value {
		if current != 0 {
			return false
		}
	}
	return true
}
