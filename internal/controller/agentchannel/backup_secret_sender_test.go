package agentchannel

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: the sole Controller send loop must deliver assignment first,
// purpose-ordered canonical slot frames second, and relinquish every plaintext
// buffer regardless of the stream retaining message objects.
func TestSendTaskAssignmentFramesAndClearsBackupSecretSlots(t *testing.T) {
	task, plan := controllerBackupSecretTask(t)
	accessSource := []byte("access")
	secretSource := []byte("secret")
	resolver := &fakeBackupSecretResolver{slots: map[agentpb.BackupSecretSlotPurpose][]byte{
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY: accessSource,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY: secretSource,
	}}
	server := NewWithPrivateTransfers(
		authorizedAuthenticator(), NewRegistry(), nil, &fakePlanResolver{plan: plan}, nil, resolver,
	)
	stream := &snapshotBackupSecretStream{}
	if err := server.sendTaskAssignment(stream, controllerMaterializationClaim(task)); err != nil {
		t.Fatalf("sendTaskAssignment() error = %v", err)
	}
	if len(stream.sent) != 7 || stream.sent[0].GetTaskAssignment() == nil {
		t.Fatalf("sent messages = %#v", stream.sent)
	}
	for index, purpose := range []agentpb.BackupSecretSlotPurpose{
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY,
	} {
		offset := 1 + index*3
		header := stream.sent[offset].GetBackupSecretSlotTransfer()
		chunk := stream.sent[offset+1].GetBackupSecretSlotTransfer()
		end := stream.sent[offset+2].GetBackupSecretSlotTransfer()
		if header.GetPurpose() != purpose || header.GetTaskId() != task.ID ||
			header.GetAssignmentId() != controllerMaterializationClaim(task).Assignment.Record.AssignmentID ||
			header.GetStepId() != task.Steps[0].ID || header.GetHeader().GetChunkCount() != 1 ||
			chunk.GetChunk().GetSequence() != 1 ||
			!bytes.Equal(chunk.GetChunk().GetContent(), [][]byte{[]byte("access"), []byte("secret")}[index]) ||
			end.GetEnd().GetChunkCount() != 1 {
			t.Fatalf("slot frame %d = %#v / %#v / %#v", index, header, chunk, end)
		}
	}
	if !allZero(accessSource) || !allZero(secretSource) {
		t.Fatal("sender retained a source alias after transmission")
	}
	if len(resolver.slots) != 0 {
		t.Fatalf("resolver retained slots = %#v", resolver.slots)
	}
}

// Rationale: a partial transport failure must clear the transferred source and
// emit no End record that could mark an incomplete slot ready.
func TestSendBackupSecretSlotClearsOwnedBufferOnSendFailure(t *testing.T) {
	content := bytes.Repeat([]byte{7}, int(executionplan.MaximumBackupSecretChunkBytes)+1)
	stream := &failingBackupSecretStream{failAt: 3}
	err := sendBackupSecretSlot(
		context.Background(),
		stream,
		"task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"step_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY,
		content,
	)
	if err == nil {
		t.Fatal("sendBackupSecretSlot() error = nil")
	}
	if !allZero(content) {
		t.Fatal("send failure retained owned secret source")
	}
	for _, message := range stream.sent {
		if message.GetBackupSecretSlotTransfer().GetEnd() != nil {
			t.Fatal("send failure emitted Backup secret End")
		}
	}
}

// Rationale: AGE uses only the current assignment's private identity. The old
// identity is restore-only and must never be requested for a Backup capture.
func TestExpectedBackupSecretSlotPurposesAgeIsExact(t *testing.T) {
	purposes, err := expectedBackupSecretSlotPurposes(&agentpb.BackupSourceCapture{
		Encryption: agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE,
	})
	if err != nil {
		t.Fatalf("expectedBackupSecretSlotPurposes() error = %v", err)
	}
	want := []agentpb.BackupSecretSlotPurpose{
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY,
	}
	if len(purposes) != len(want) {
		t.Fatalf("purposes = %v, want %v", purposes, want)
	}
	for index := range want {
		if purposes[index] != want[index] {
			t.Fatalf("purposes = %v, want %v", purposes, want)
		}
	}
}

type fakeBackupSecretResolver struct {
	slots map[agentpb.BackupSecretSlotPurpose][]byte
}

func (resolver *fakeBackupSecretResolver) ResolveBackupSecretSlots(
	context.Context,
	etcd.TaskRecord,
	*agentpb.ExecutionPlan,
	*agentpb.ExecutionStep,
) (map[agentpb.BackupSecretSlotPurpose][]byte, error) {
	return resolver.slots, nil
}

type failingBackupSecretStream struct {
	scriptedStream
	calls  int
	failAt int
}

type snapshotBackupSecretStream struct {
	scriptedStream
}

func (stream *snapshotBackupSecretStream) Send(message *agentpb.ControllerMessage) error {
	stream.sent = append(stream.sent, proto.Clone(message).(*agentpb.ControllerMessage))
	return nil
}

func (stream *failingBackupSecretStream) Send(message *agentpb.ControllerMessage) error {
	stream.calls++
	if stream.calls == stream.failAt {
		return errors.New("injected backup secret send failure")
	}
	return stream.scriptedStream.Send(message)
}

func controllerBackupSecretTask(t *testing.T) (etcd.TaskRecord, *agentpb.ExecutionPlan) {
	t.Helper()
	const (
		taskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		operationID   = "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		planID        = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		stepID        = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: planID,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP, TargetId: environmentID,
		Steps: []*agentpb.ExecutionStep{{
			StepId: stepID, TimeoutSeconds: 60,
			Payload: &agentpb.ExecutionStep_BackupSourceCapture{BackupSourceCapture: &agentpb.BackupSourceCapture{
				SourceId: "spt_01ARZ3NDEKTSV4RRFFQ69G5FAV", SourceRevision: 2,
				TargetId: "att_01ARZ3NDEKTSV4RRFFQ69G5FAV", TargetRevision: 3,
				PointId: "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV", ConnectorId: "con_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				ConnectorRevision: 4,
				SourceFormat:      agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_POSTGRES_CUSTOM_V1,
				Encryption:        agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE,
				Source: &agentpb.BackupSourceCapture_Attach{Attach: &agentpb.BackupAttachSource{
					BackingServiceId: "bks_01ARZ3NDEKTSV4RRFFQ69G5FAV", BackingServiceRevision: 5,
					Database: "application", Role: "application_owner",
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Seal(Backup) error = %v", err)
	}
	return etcd.TaskRecord{
		ID: taskID, OperationID: operationID, PlanID: planID, PlanHash: hex.EncodeToString(plan.PlanHash),
		Type: etcd.TaskBackup, Target: environmentID, Steps: []etcd.TaskStepRecord{{ID: stepID}},
		TimeoutSeconds: 120, Status: etcd.TaskStatusRunning, NextEventSequence: 1,
		CreatedAt: time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC),
	}, plan
}

func allZero(value []byte) bool {
	for _, current := range value {
		if current != 0 {
			return false
		}
	}
	return true
}
