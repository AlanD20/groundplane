package agentchannel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// TASK-49: each requested source carries its own attempt and ordinal, in purpose
// order. A transport retaining message pointers must not retain plaintext aliases.
func TestSendBackupWorkSecretsBindsSourcesAndClearsBuffers(t *testing.T) {
	input, attempt, identity := backupSecretSenderFixture(t)
	for ordinal := range input.Steps {
		access := []byte(fmt.Sprintf("access-source-%d", ordinal))
		secret := bytes.Repeat([]byte{byte(ordinal + 1)}, int(executionplan.MaximumBackupSecretChunkBytes)+1)
		wantAccess, wantSecret := bytes.Clone(access), bytes.Clone(secret)
		slots := backupSenderSlots(access, secret)
		stream := &backupSecretSendStream{ctx: t.Context()}
		if err := sendBackupWorkSecrets(t.Context(), stream, input, attempt, uint32(ordinal), identity, slots); err != nil {
			t.Fatal(err)
		}
		if len(stream.sent) != 7 {
			t.Fatalf("source %d: got %d frames, want 7", ordinal, len(stream.sent))
		}
		for _, expected := range []struct {
			start, end int
			purpose    agentpb.BackupSecretSlotPurpose
			content    []byte
		}{
			{0, 2, agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY, wantAccess},
			{3, 6, agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY, wantSecret},
		} {
			header := stream.sent[expected.start].GetBackupWorkSecretTransfer()
			if header.GetHeader() == nil || header.Purpose != expected.purpose ||
				header.GetHeader().TotalBytes != uint64(len(expected.content)) {
				t.Fatalf("source %d: wrong credential header", ordinal)
			}
			var content []byte
			for index := expected.start; index <= expected.end; index++ {
				frame := stream.sent[index].GetBackupWorkSecretTransfer()
				if !proto.Equal(frame.Attempt, attempt) || frame.SourceOrdinal != uint32(ordinal) ||
					frame.Purpose != expected.purpose {
					t.Fatalf("source %d frame %d belongs to another credential request", ordinal, index)
				}
				if chunk := frame.GetChunk(); chunk != nil {
					if chunk.Sequence != uint32(index-expected.start) {
						t.Fatalf("source %d frame %d: out-of-order chunk", ordinal, index)
					}
					content = append(content, chunk.Content...)
				}
			}
			if !bytes.Equal(content, expected.content) ||
				stream.sent[expected.end].GetBackupWorkSecretTransfer().
					GetEnd().
					GetChunkCount() !=
					uint32(
						expected.end-expected.start-1,
					) {
				t.Fatalf("source %d: incomplete credential transfer", ordinal)
			}
		}
		assertBackupSenderCleared(t, stream, slots, access, secret)
	}
}

// TASK-49: failure at any transport boundary must clear all credentials, even
// untransmitted slots. Cancellation must not finish a partially transmitted slot.
func TestSendBackupWorkSecretsClearsOnFailureAndCancellation(t *testing.T) {
	for failAt := 1; failAt <= 7; failAt++ {
		t.Run(fmt.Sprintf("send-%d", failAt), func(t *testing.T) {
			input, attempt, identity := backupSecretSenderFixture(t)
			access := []byte("access")
			secret := bytes.Repeat([]byte{7}, int(executionplan.MaximumBackupSecretChunkBytes)+1)
			slots := backupSenderSlots(access, secret)
			stream := &backupSecretSendStream{ctx: t.Context(), failAt: failAt}
			err := sendBackupWorkSecrets(t.Context(), stream, input, attempt, 0, identity, slots)
			if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) || stream.calls != failAt {
				t.Fatalf("failure was not propagated: calls=%d, err=%v", stream.calls, err)
			}
			failedPurpose := agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY
			if failAt > 3 {
				failedPurpose = agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY
			}
			for _, sent := range stream.sent {
				if frame := sent.GetBackupWorkSecretTransfer(); frame.Purpose == failedPurpose &&
					frame.GetEnd() != nil {
					t.Fatal("incomplete credential was marked complete")
				}
			}
			assertBackupSenderCleared(t, stream, slots, access, secret)
		})
	}
	for _, cancelAt := range []int{0, 2} {
		t.Run(fmt.Sprintf("cancel-%d", cancelAt), func(t *testing.T) {
			input, attempt, identity := backupSecretSenderFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			access, secret := []byte("access"), []byte("secret")
			slots := backupSenderSlots(access, secret)
			stream := &backupSecretSendStream{ctx: ctx, cancelAt: cancelAt, cancel: cancel}
			if cancelAt == 0 {
				cancel()
			}
			err := sendBackupWorkSecrets(ctx, stream, input, attempt, 0, identity, slots)
			if !errors.Is(err, context.Canceled) || stream.calls != cancelAt {
				t.Fatalf("cancellation was not respected: calls=%d, err=%v", stream.calls, err)
			}
			assertBackupSenderCleared(t, stream, slots, access, secret)
		})
	}
}

// TASK-49: reject the entire slot set before emitting any bytes; the resolver
// cannot expand privilege, omit a slot or choose another source or Agent process.
func TestSendBackupWorkSecretsRejectsInvalidSetBeforeSending(t *testing.T) {
	for _, name := range []string{"missing", "extra identity", "substituted identity", "empty", "oversized", "foreign Agent", "unknown source"} {
		t.Run(name, func(t *testing.T) {
			input, attempt, identity := backupSecretSenderFixture(t)
			access, secret, age := []byte("access"), []byte("secret"), []byte("age-identity")
			slots := backupSenderSlots(access, secret)
			ordinal := uint32(0)
			switch name {
			case "missing":
				slots = map[agentpb.BackupSecretSlotPurpose][]byte{
					agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY: access,
				}
				secret = nil
			case "extra identity", "substituted identity":
				slots[agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_OPERATOR_OLD_AGE_IDENTITY] = age
				if name == "substituted identity" {
					delete(slots, agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY)
					secret = nil
				}
			case "empty":
				secret = nil
				slots[agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY] = secret
			case "oversized":
				secret = bytes.Repeat([]byte{7}, int(executionplan.MaximumBackupSecretCredentialBytes)+1)
				slots[agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY] = secret
			case "foreign Agent":
				identity.Generation++
			case "unknown source":
				ordinal = uint32(len(input.Steps))
			}
			stream := &backupSecretSendStream{ctx: t.Context()}
			if err := sendBackupWorkSecrets(t.Context(), stream, input, attempt, ordinal, identity, slots); err == nil ||
				stream.calls != 0 {
				t.Fatalf("invalid request transmitted plaintext: calls=%d, err=%v", stream.calls, err)
			}
			assertBackupSenderCleared(t, stream, slots, access, secret)
			if (name == "extra identity" || name == "substituted identity") && !allZero(age) {
				t.Fatal("rejected identity retained plaintext")
			}
		})
	}
}

func backupSenderSlots(access, secret []byte) map[agentpb.BackupSecretSlotPurpose][]byte {
	return map[agentpb.BackupSecretSlotPurpose][]byte{
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY: access,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY: secret,
	}
}

func assertBackupSenderCleared(t *testing.T, stream *backupSecretSendStream,
	slots map[agentpb.BackupSecretSlotPurpose][]byte, buffers ...[]byte,
) {
	t.Helper()
	if len(slots) != 0 {
		t.Fatal("sender retained credential slots")
	}
	for _, buffer := range append(buffers, stream.aliases...) {
		if !allZero(buffer) {
			t.Fatal("sender retained a plaintext alias")
		}
	}
	for _, frame := range stream.retained {
		if frame.GetBackupWorkSecretTransfer().GetChunk().GetContent() != nil {
			t.Fatal("retained transport message still exposes credential content")
		}
	}
}

type backupSecretSendStream struct {
	ctx            context.Context
	calls, failAt  int
	cancelAt       int
	cancel         context.CancelFunc
	sent, retained []*agentpb.ControllerMessage
	aliases        [][]byte
}

func (stream *backupSecretSendStream) Send(message *agentpb.ControllerMessage) error {
	stream.calls++
	stream.retained = append(stream.retained, message)
	if chunk := message.GetBackupWorkSecretTransfer().GetChunk(); chunk != nil {
		stream.aliases = append(stream.aliases, chunk.Content)
	}
	if stream.calls == stream.failAt {
		return errs.New(errs.KindStorageUnavailable, "injected credential send failure")
	}
	stream.sent = append(stream.sent, proto.CloneOf(message))
	if stream.calls == stream.cancelAt && stream.cancel != nil {
		stream.cancel()
	}
	return nil
}

func (stream *backupSecretSendStream) Context() context.Context { return stream.ctx }
func (*backupSecretSendStream) SetHeader(metadata.MD) error {
	return errs.New(errs.KindInternal, "unexpected transport header")
}
func (*backupSecretSendStream) SendHeader(metadata.MD) error {
	return errs.New(errs.KindInternal, "unexpected transport header")
}
func (*backupSecretSendStream) SetTrailer(metadata.MD) {}
func (*backupSecretSendStream) SendMsg(any) error {
	return errs.New(errs.KindInternal, "unexpected untyped transport send")
}
func (*backupSecretSendStream) RecvMsg(any) error {
	return errs.New(errs.KindInternal, "unexpected untyped transport receive")
}
func (*backupSecretSendStream) Recv() (*agentpb.AgentMessage, error) {
	return nil, errs.New(errs.KindInternal, "unexpected credential sender receive")
}

func backupSecretSenderFixture(
	t *testing.T,
) (*agentpb.BackupWorkInput, *agentpb.WorkAttemptIdentity, workexecution.Identity) {
	t.Helper()
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	input := &agentpb.BackupWorkInput{Schema: 1, Operation: agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE,
		ProjectId: ids.New(ids.KindProject), Project: backupSenderRevision(1),
		EnvironmentId: ids.New(ids.KindEnvironment), Environment: backupSenderRevision(2)}
	for index := byte(1); index <= 2; index++ {
		sourceID, pointID := ids.New(ids.KindBackupSource), ids.New(ids.KindRecoveryPoint)
		step, err := executionplan.SealBackupStepAuthority(&agentpb.BackupStepAuthority{
			StepId: ids.New(ids.KindStep), ExecutionId: ids.NewULID(),
			StepDeadlineUnixNano: uint64(at.Add(time.Minute).UnixNano()),
			Operation: &agentpb.BackupStepAuthority_Prune{Prune: &agentpb.BackupPruneAuthority{
				RetentionPolicy: backupSenderRevision(20),
				Objects: []*agentpb.BackupPruneObject{{Ordinal: uint32(index), PointId: pointID,
					Point: backupSenderRevision(index + 30), Evidence: &agentpb.BackupArtifactEvidence{
						SourceSizeBytes: 4096, SourceSha256: bytes.Repeat([]byte{index}, sha256.Size),
						StoredSizeBytes: 4096, StoredSha256: bytes.Repeat([]byte{index}, sha256.Size)},
					Object: &agentpb.BackupObjectIdentity{Connector: &agentpb.BackupConnectorAuthority{
						ConnectorId: ids.New(ids.KindConnector), Connector: backupSenderRevision(40),
						CanonicalEndpointUrl: "https://objects.example.test", Region: "auto", PathStyle: proto.Bool(true),
						Prefix: "backups/", AccessKeySlotId: backupsecret.AccessKeySlotID, SecretKeySlotId: backupsecret.SecretKeySlotID,
						AccessKeySlot: backupSenderRevision(41), SecretKeySlot: backupSenderRevision(42)},
						Bucket: "groundplane-backups", ObjectKey: "backups/" + input.EnvironmentId + "/" + sourceID + "/" + pointID + "/artifact.bin",
						Discriminator: &agentpb.BackupObjectIdentity_Etag{Etag: &agentpb.BackupS3ETag{Value: "etag"}}},
					MetadataCount: 10, MetadataSha256: bytes.Repeat([]byte{50}, sha256.Size)}},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		input.Steps = append(input.Steps, step)
	}
	input, raw, reference, err := executionplan.CaptureBackupWorkInput(ids.New(ids.KindPlan), input)
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: ids.New(ids.KindHost),
		AgentID: ids.New(ids.KindAgent), Generation: 1}
	wire, err := workexecution.EncodeAgentAttempt(workexecution.Attempt{ID: ids.New(ids.KindAttempt),
		TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem), Number: 1,
		Executor: identity, Input: reference, Direction: workexecution.Forward, AssignedAt: at, StartedAt: &at,
		Deadline: at.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	return input, wire, identity
}

func backupSenderRevision(fill byte) *agentpb.RevisionDigest {
	return &agentpb.RevisionDigest{ModRevision: int64(fill), Sha256: bytes.Repeat([]byte{fill}, sha256.Size)}
}

func allZero(value []byte) bool {
	for _, current := range value {
		if current != 0 {
			return false
		}
	}
	return true
}
