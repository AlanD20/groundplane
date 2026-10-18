package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/backupruntime"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TASK-51: the native session receiver clears received bytes on acceptance and
// rejection, while transferring an independent owned copy to the source consumer.
func TestNativeBackupSecretSessionClearsIncomingChunks(t *testing.T) {
	assignment, exchange := nativeBackupSecretSession(t)
	if err := exchange.RegisterBackupSecrets(assignment); err != nil {
		t.Fatal(err)
	}
	frames := nativeBackupSecretFrames(assignment, []byte("access"))
	for _, frame := range frames {
		alias := frame.GetChunk().GetContent()
		if err := exchange.acceptBackupSecrets(t.Context(), frame); err != nil {
			t.Fatal(err)
		}
		assertNativeSecretCleared(t, alias)
		if frame.GetChunk().GetContent() != nil {
			t.Fatal("session retained a received chunk")
		}
	}
	var consumed []byte
	if err := exchange.ConsumeBackupSecret(t.Context(), assignment.Attempt, 0,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY, func(value []byte) error {
			consumed = value
			if !bytes.Equal(value, []byte("access")) {
				t.Fatal("session changed received credentials")
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}
	assertNativeSecretCleared(t, consumed)
	foreign := nativeBackupSecretFrames(assignment, []byte("foreign"))[1]
	foreign.Attempt.AttemptId = ids.New(ids.KindAttempt)
	alias := foreign.GetChunk().Content
	if err := exchange.acceptBackupSecrets(t.Context(), foreign); err == nil {
		t.Fatal("session admitted a foreign credential chunk")
	}
	assertNativeSecretCleared(t, alias)
	if foreign.GetChunk().Content != nil {
		t.Fatal("session retained rejected chunk content")
	}
}

// TASK-51: cancellation and stream teardown wake an incomplete consumer. Neither
// path may invoke the consumer or strand it waiting for an End that cannot arrive.
func TestNativeBackupSecretSessionWakesBlockedConsumer(t *testing.T) {
	for _, closeStream := range []bool{false, true} {
		name := "cancel"
		if closeStream {
			name = "stream closed"
		}
		t.Run(name, func(t *testing.T) {
			assignment, exchange := nativeBackupSecretSession(t)
			if err := exchange.RegisterBackupSecrets(assignment); err != nil {
				t.Fatal(err)
			}
			frames := nativeBackupSecretFrames(assignment, []byte("partial"))
			for _, frame := range frames[:2] {
				if err := exchange.acceptBackupSecrets(t.Context(), frame); err != nil {
					t.Fatal(err)
				}
			}
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			attempt, err := workexecution.DecodeAgentAttempt(assignment.Attempt, exchange.identity)
			if err != nil {
				t.Fatal(err)
			}
			workers := &executionWorkers{ctx: parent, identity: exchange.identity,
				active: map[string]activeExecution{attempt.WorkItemID: {attempt: attempt}}}
			ctx := workers.executionContext(attempt)
			observed := &nativeSecretWaitContext{Context: ctx, observed: make(chan struct{})}
			done := make(chan error, 1)
			go func() {
				done <- exchange.ConsumeBackupSecret(observed, assignment.Attempt, 0,
					agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
					func([]byte) error { return errs.New(errs.KindInternal, "incomplete credential was consumed") })
			}()
			select {
			case <-observed.observed:
			case <-time.After(2 * time.Second):
				t.Fatal("consumer did not enter its wait")
			}
			if closeStream {
				exchange.close()
			} else {
				stale := proto.CloneOf(assignment.Attempt)
				stale.AttemptId = ids.New(ids.KindAttempt)
				if err := workers.cancelAttempt(&agentpb.WorkCancel{Attempt: stale}); err != nil || ctx.Err() != nil {
					t.Fatalf("stale cancellation changed the active attempt: %v", err)
				}
				if err := workers.cancelAttempt(&agentpb.WorkCancel{Attempt: assignment.Attempt}); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) && !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
					t.Fatalf("blocked credential consumer: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("credential consumer remained blocked")
			}
		})
	}
}

// TASK-51: a replacement stream starts with no old slots, even when the native
// attempt resumes. Its explicit request and newly received bytes own the new slot.
func TestNativeBackupSecretSessionReconnectRequiresFreshBytes(t *testing.T) {
	assignment, old := nativeBackupSecretSession(t)
	if err := old.RegisterBackupSecrets(assignment); err != nil {
		t.Fatal(err)
	}
	for _, frame := range nativeBackupSecretFrames(assignment, []byte("old-access")) {
		if err := old.acceptBackupSecrets(t.Context(), frame); err != nil {
			t.Fatal(err)
		}
	}
	old.close()
	if err := old.ConsumeBackupSecret(t.Context(), assignment.Attempt, 0,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY, func([]byte) error { return nil }); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("old stream retained its slot: %v", err)
	}
	fresh := newNativeExchange(t.Context(), nil)
	t.Cleanup(fresh.close)
	fresh.identity = old.identity
	if err := fresh.RegisterBackupSecrets(assignment); err != nil {
		t.Fatal(err)
	}
	if err := fresh.RequestBackupSecrets(t.Context(), assignment.Attempt, 0); err != nil {
		t.Fatal(err)
	}
	request := <-fresh.outgoing
	if got := request.GetBackupWorkSecretRequest(); got == nil || !proto.Equal(got.Attempt, assignment.Attempt) ||
		got.SourceOrdinal != 0 {
		t.Fatal("reconnect requested different captured credentials")
	}
	for _, frame := range nativeBackupSecretFrames(assignment, []byte("fresh-access")) {
		if err := fresh.acceptBackupSecrets(t.Context(), frame); err != nil {
			t.Fatal(err)
		}
	}
	if err := fresh.ConsumeBackupSecret(t.Context(), assignment.Attempt, 0,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY, func(value []byte) error {
			if !bytes.Equal(value, []byte("fresh-access")) {
				t.Fatal("replacement stream consumed stale credentials")
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}
}

// TASK-51: while terminal delivery is awaiting its ACK, the native executor must
// already reject late secret frames. Report transport failure must not prolong
// the lifetime of a partly received credential or permit more source execution.
func TestNativeBackupExecutorReleasesSecretsBeforeFailureDelivery(t *testing.T) {
	assignment, exchange := nativeBackupSecretSession(t)
	journal, err := agentworkjournal.Open(filepath.Join(t.TempDir(), "work"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := journal.Close(); err != nil {
			t.Error(err)
		}
	})
	exchange.journal = journal
	executor, err := backupruntime.NewExecutor(journal, "/var/lib/groundplane/vol",
		unusedBackupSecretHost{}, unusedBackupSecretHost{}, unusedBackupSecretHost{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, runErr := executor.Run(ctx, assignment, exchange.identity, exchange, nil)
		done <- runErr
	}()
	defer func() {
		cancel()
		exchange.close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("native Backup executor did not stop after its stream closed")
		}
	}()
	start := nextNativeSecretRequest(t, exchange).GetWorkStartRequest()
	if start == nil {
		t.Fatal("native executor did not request Start")
	}
	handled, err := exchange.accept(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_WorkStartAck{
		WorkStartAck: &agentpb.WorkStartAck{Attempt: proto.CloneOf(start.Attempt), StartedAt: timestamppb.Now()}}})
	if err != nil || !handled {
		t.Fatalf("native start acknowledgement: %v", err)
	}
	request := nextNativeSecretRequest(t, exchange).GetBackupWorkSecretRequest()
	if request == nil {
		t.Fatal("native executor did not request credentials")
	}
	secretFrames := nativeBackupSecretFrames(assignment, []byte("buffered-secret"))
	for _, frame := range secretFrames {
		frame.Purpose = agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY
		if err := exchange.acceptBackupSecrets(t.Context(), frame); err != nil {
			t.Fatal(err)
		}
	}
	frames := nativeBackupSecretFrames(assignment, []byte("partial-access"))
	if err := exchange.acceptBackupSecrets(t.Context(), frames[0]); err != nil {
		t.Fatal(err)
	}
	frames[1].GetChunk().Sequence = 2
	if err := exchange.acceptBackupSecrets(t.Context(), frames[1]); err == nil {
		t.Fatal("out-of-order access credential was accepted")
	}
	if nextNativeSecretRequest(t, exchange).GetBackupWorkFailureRequest() == nil {
		t.Fatal("native executor did not preserve its failed outcome")
	}
	savedAttempt, saved, err := journal.LoadBackup(t.Context(), assignment, exchange.identity)
	if err != nil || savedAttempt == nil || saved == nil ||
		saved.FailureReason != executionplan.BackupExecutionFailed ||
		saved.Completion != nil {
		t.Fatalf("failure report lacks its durable original outcome: %v", err)
	}
	if err := exchange.ConsumeBackupSecret(t.Context(), assignment.Attempt, 0,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY, func([]byte) error { return nil }); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("failure delivery retained the other credential: %v", err)
	}
}

func nextNativeSecretRequest(t *testing.T, exchange *nativeExchange) *agentpb.AgentMessage {
	t.Helper()
	select {
	case message := <-exchange.outgoing:
		return message
	case <-time.After(2 * time.Second):
		t.Fatal("native executor did not emit its next request")
		return nil
	}
}

type nativeSecretWaitContext struct {
	context.Context
	once     sync.Once
	observed chan struct{}
}

func (ctx *nativeSecretWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.observed) })
	return ctx.Context.Done()
}

type unusedBackupSecretHost struct{}

func (unusedBackupSecretHost) Check() error { return nil }

func (unusedBackupSecretHost) ExecuteBackupConsumer(
	context.Context,
	*agentpb.BackupConsumerHelperRequest,
) (*agentpb.BackupConsumerHelperResponse, error) {
	return nil, errs.New(errs.KindInternal, "credential-only test must not execute a consumer")
}

func (unusedBackupSecretHost) RunConfigRestoreFile(
	context.Context,
	string,
	entrymaterialization.Header,
	io.ReadCloser,
	bool,
) error {
	return errs.New(errs.KindInternal, "credential-only test must not restore files")
}

func nativeBackupSecretSession(t *testing.T) (*agentpb.BackupWorkAssignment, *nativeExchange) {
	t.Helper()
	at := time.Now().UTC().Add(-time.Second)
	deadline := at.Add(time.Minute)
	revision := func(fill byte) *agentpb.RevisionDigest {
		return &agentpb.RevisionDigest{ModRevision: int64(fill), Sha256: bytes.Repeat([]byte{fill}, sha256.Size)}
	}
	step, err := executionplan.SealBackupStepAuthority(&agentpb.BackupStepAuthority{
		StepId: ids.New(ids.KindStep), ExecutionId: ids.NewULID(), StepDeadlineUnixNano: uint64(deadline.UnixNano()),
		Operation: &agentpb.BackupStepAuthority_Prune{Prune: &agentpb.BackupPruneAuthority{
			RetentionPolicy: revision(1), Objects: []*agentpb.BackupPruneObject{{Ordinal: 1,
				PointId: "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Point: revision(2),
				Evidence: &agentpb.BackupArtifactEvidence{SourceSizeBytes: 4096, StoredSizeBytes: 4096,
					SourceSha256: bytes.Repeat([]byte{1}, 32), StoredSha256: bytes.Repeat([]byte{1}, 32)},
				Object: &agentpb.BackupObjectIdentity{Connector: &agentpb.BackupConnectorAuthority{
					ConnectorId: ids.New(ids.KindConnector), Connector: revision(3),
					CanonicalEndpointUrl: "https://objects.example.test", Region: "auto", PathStyle: proto.Bool(true),
					Prefix: "qa/", AccessKeySlotId: backupsecret.AccessKeySlotID, SecretKeySlotId: backupsecret.SecretKeySlotID,
					AccessKeySlot: revision(4), SecretKeySlot: revision(5)}, Bucket: "groundplane-backups",
					ObjectKey:     "qa/env_01ARZ3NDEKTSV4RRFFQ69G5FAV/spt_01ARZ3NDEKTSV4RRFFQ69G5FAV/rp_01ARZ3NDEKTSV4RRFFQ69G5FAV/artifact.bin",
					Discriminator: &agentpb.BackupObjectIdentity_Etag{Etag: &agentpb.BackupS3ETag{Value: "etag"}}},
				MetadataCount: 10, MetadataSha256: bytes.Repeat([]byte{6}, 32)}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	input, raw, reference, err := executionplan.CaptureBackupWorkInput(ids.New(ids.KindPlan), &agentpb.BackupWorkInput{
		Schema: 1, Operation: agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE,
		ProjectId: ids.New(
			ids.KindProject,
		), Project: revision(6), EnvironmentId: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", Environment: revision(7),
		Steps: []*agentpb.BackupStepAuthority{step}})
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	identity := workexecution.Identity{
		Executor:   workexecution.Agent,
		HostID:     ids.New(ids.KindHost),
		AgentID:    ids.New(ids.KindAgent),
		Generation: 1,
	}
	wire, err := workexecution.EncodeAgentAttempt(workexecution.Attempt{ID: ids.New(ids.KindAttempt),
		TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem), Number: 1,
		Executor: identity, Input: reference, Direction: workexecution.Forward, AssignedAt: at, Deadline: deadline})
	if err != nil {
		t.Fatal(err)
	}
	assignment := &agentpb.BackupWorkAssignment{Attempt: wire, Input: input, Sources: []*agentpb.BackupStepResume{{
		StepId: step.StepId, ExecutionId: step.ExecutionId,
		Operation: &agentpb.BackupStepResume_Prune{Prune: &agentpb.BackupPruneResume{NextObjectOrdinal: 1}}}}}
	exchange := newNativeExchange(t.Context(), nil)
	exchange.identity = identity
	t.Cleanup(exchange.close)
	return assignment, exchange
}

func nativeBackupSecretFrames(
	assignment *agentpb.BackupWorkAssignment,
	value []byte,
) []*agentpb.BackupWorkSecretTransfer {
	frame := func() *agentpb.BackupWorkSecretTransfer {
		return &agentpb.BackupWorkSecretTransfer{Attempt: proto.CloneOf(assignment.Attempt), SourceOrdinal: 0,
			Purpose: agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY}
	}
	header, chunk, end := frame(), frame(), frame()
	header.Record = &agentpb.BackupWorkSecretTransfer_Header{
		Header: &agentpb.BackupSecretSlotHeader{TotalBytes: uint64(len(value)), ChunkCount: 1},
	}
	chunk.Record = &agentpb.BackupWorkSecretTransfer_Chunk{
		Chunk: &agentpb.BackupSecretSlotChunk{Sequence: 1, Content: bytes.Clone(value)},
	}
	end.Record = &agentpb.BackupWorkSecretTransfer_End{End: &agentpb.BackupSecretSlotEnd{ChunkCount: 1}}
	return []*agentpb.BackupWorkSecretTransfer{header, chunk, end}
}

func assertNativeSecretCleared(t *testing.T, value []byte) {
	t.Helper()
	for _, octet := range value {
		if octet != 0 {
			t.Fatal("native session retained credential bytes")
		}
	}
}
