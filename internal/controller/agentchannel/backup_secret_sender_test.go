package agentchannel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
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
	if err := sendTestTaskAssignment(server, stream, controllerMaterializationClaim(task)); err != nil {
		t.Fatalf("sendTestTaskAssignment() error = %v", err)
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
			header.GetAssignmentId() != controllerMaterializationClaim(
				task,
			).Assignment.Record.AssignmentID ||
			header.GetStepId() != task.Steps[0].ID || header.GetHeader().GetChunkCount() != 1 ||
			chunk.GetChunk().GetSequence() != 1 ||
			!bytes.Equal(
				chunk.GetChunk().GetContent(),
				[][]byte{[]byte("access"), []byte("secret")}[index],
			) ||
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
	claim := controllerMaterializationClaim(task)
	if len(resolver.requests) != 1 || resolver.requests[0].TaskID != task.ID ||
		resolver.requests[0].AssignmentID != claim.Assignment.Record.AssignmentID ||
		resolver.requests[0].AgentID != claim.Assignment.Record.AgentID ||
		resolver.requests[0].AgentGeneration != claim.Assignment.Record.AgentGeneration ||
		!resolver.requests[0].Deadline.Equal(claim.Assignment.Record.Deadline) ||
		resolver.requests[0].StepID != task.Steps[0].ID {
		t.Fatalf("resolver requests = %#v, want exact active assignment", resolver.requests)
	}
}

// Rationale: resolved mixed credential sources must become exactly two ordered
// frames and the channel must clear every plaintext buffer it accepts.
func TestSendBackupPruneSlotsWithMixedResolvedEvidence(t *testing.T) {
	task, plan := controllerBackupPruneSecretTask(t)
	accessSource := []byte("direct-access")
	secretSource := []byte("project-secret")
	resolver := &fakeBackupSecretResolver{slots: map[agentpb.BackupSecretSlotPurpose][]byte{
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY: accessSource,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY: secretSource,
	}}
	server := NewWithPrivateTransfers(
		authorizedAuthenticator(), NewRegistry(), nil, nil, nil, resolver,
	)
	stream := &snapshotBackupSecretStream{}
	request := backupsecret.Request{
		TaskID: task.ID, AssignmentID: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		AgentID: "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV", AgentGeneration: 1,
		Deadline: task.CreatedAt.Add(time.Minute), StepID: plan.Steps[0].StepId,
		Plan: plan, Step: plan.Steps[0],
	}
	if err := server.sendBackupSecretSlots(context.Background(), stream, request); err != nil {
		t.Fatalf("sendBackupSecretSlots(real resolver): %v", err)
	}
	if len(stream.sent) != 6 {
		t.Fatalf("sent messages = %d, want 6", len(stream.sent))
	}
	for index, want := range []struct {
		purpose agentpb.BackupSecretSlotPurpose
		value   string
	}{
		{agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY, "direct-access"},
		{agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY, "project-secret"},
	} {
		offset := index * 3
		header := stream.sent[offset].GetBackupSecretSlotTransfer()
		chunk := stream.sent[offset+1].GetBackupSecretSlotTransfer()
		end := stream.sent[offset+2].GetBackupSecretSlotTransfer()
		if header.GetPurpose() != want.purpose || header.GetHeader() == nil ||
			string(chunk.GetChunk().GetContent()) != want.value ||
			end.GetEnd() == nil {
			t.Fatalf("slot %d frames = %#v/%#v/%#v", index, header, chunk, end)
		}
	}
	if !allZero(accessSource) || !allZero(secretSource) {
		t.Fatal("channel retained resolved backup credential plaintext")
	}
}

// Rationale: a valid internal prune plan must pass the durable Task/plan
// operation fence and then deliver the exact S3 slot pair for every prune step.
func TestSendTaskAssignmentDispatchesBackupPruneSecretSlotsPerStep(t *testing.T) {
	task, plan := controllerBackupPruneSecretTask(t)
	wantContents := [2][2][]byte{
		{[]byte("access-step-1"), []byte("secret-step-1")},
		{[]byte("access-step-2"), []byte("secret-step-2")},
	}
	accessSources := [][]byte{
		append([]byte(nil), wantContents[0][0]...),
		append([]byte(nil), wantContents[1][0]...),
	}
	secretSources := [][]byte{
		append([]byte(nil), wantContents[0][1]...),
		append([]byte(nil), wantContents[1][1]...),
	}
	resolver := &sequentialBackupSecretResolver{
		slotSets: []map[agentpb.BackupSecretSlotPurpose][]byte{
			{
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY: accessSources[0],
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY: secretSources[0],
			},
			{
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY: accessSources[1],
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY: secretSources[1],
			},
		},
	}
	server := NewWithPrivateTransfers(
		authorizedAuthenticator(), NewRegistry(), nil, &fakePlanResolver{plan: plan}, nil, resolver,
	)
	stream := &snapshotBackupSecretStream{}
	if err := sendTestTaskAssignment(server, stream, controllerMaterializationClaim(task)); err != nil {
		t.Fatalf("sendTestTaskAssignment() error = %v", err)
	}
	assignment := stream.sent[0].GetTaskAssignment()
	if assignment == nil ||
		assignment.GetPlan().GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE {
		t.Fatalf("assignment = %#v, want BACKUP_PRUNE plan", stream.sent[0])
	}
	if len(stream.sent) != 13 {
		t.Fatalf("sent messages = %d, want 13", len(stream.sent))
	}
	for stepIndex, step := range plan.GetSteps() {
		stepOffset := 1 + stepIndex*6
		for purposeIndex, purpose := range []agentpb.BackupSecretSlotPurpose{
			agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
			agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY,
		} {
			offset := stepOffset + purposeIndex*3
			header := stream.sent[offset].GetBackupSecretSlotTransfer()
			chunk := stream.sent[offset+1].GetBackupSecretSlotTransfer()
			end := stream.sent[offset+2].GetBackupSecretSlotTransfer()
			wantContent := wantContents[stepIndex][purposeIndex]
			if header.GetPurpose() != purpose || header.GetStepId() != step.GetStepId() ||
				header.GetHeader().GetChunkCount() != 1 || chunk.GetChunk().GetSequence() != 1 ||
				!bytes.Equal(
					chunk.GetChunk().GetContent(),
					wantContent,
				) || end.GetEnd().GetChunkCount() != 1 {
				t.Fatalf(
					"step %d purpose %d frames = %#v / %#v / %#v",
					stepIndex,
					purposeIndex,
					header,
					chunk,
					end,
				)
			}
		}
	}
	if len(resolver.slotSets) != 0 {
		t.Fatalf("resolver retained slot sets = %d, want 0", len(resolver.slotSets))
	}
	for _, source := range append(accessSources, secretSources...) {
		if !allZero(source) {
			t.Fatal("prune dispatch retained a source alias")
		}
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

// Rationale: the sealed encryption authority selects the exact identity slot:
// age capture receives the current identity, while unencrypted capture and
// prune receive only the S3 credential pair.
func TestExpectedBackupSecretSlotPurposesAreLeastPrivilege(t *testing.T) {
	for _, test := range []struct {
		name string
		step *agentpb.ExecutionStep
		want []agentpb.BackupSecretSlotPurpose
	}{
		{
			name: "age capture",
			step: backupSecretPurposeCaptureStep(&agentpb.BackupEncryptionAuthority{
				Kind:         agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE,
				SecretSlotId: backupsecret.CurrentAgeIdentitySlotID,
			}),
			want: []agentpb.BackupSecretSlotPurpose{
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY,
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY,
			},
		},
		{
			name: "unencrypted capture",
			step: backupSecretPurposeCaptureStep(&agentpb.BackupEncryptionAuthority{
				Kind: agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE,
			}),
			want: []agentpb.BackupSecretSlotPurpose{
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY,
			},
		},
		{
			name: "prune",
			step: &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_BackupStep{
				BackupStep: &agentpb.BackupStepAuthority{Operation: &agentpb.BackupStepAuthority_Prune{
					Prune: &agentpb.BackupPruneAuthority{},
				}},
			}},
			want: []agentpb.BackupSecretSlotPurpose{
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			purposes, err := expectedBackupSecretSlotPurposes(test.step)
			if err != nil {
				t.Fatalf("expectedBackupSecretSlotPurposes() error = %v", err)
			}
			if len(purposes) != len(test.want) {
				t.Fatalf("purposes = %v, want %v", purposes, test.want)
			}
			for index := range test.want {
				if purposes[index] != test.want[index] {
					t.Fatalf("purposes = %v, want %v", purposes, test.want)
				}
			}
		})
	}
}

// Rationale: a resolver is not trusted to choose privilege. Returning either
// identity when the sealed step selected none must fail before any frame is
// sent and still relinquish every plaintext buffer.
func TestSendBackupSecretSlotsRejectsRestoreIdentityPurposes(t *testing.T) {
	for _, stepKind := range []string{"capture", "prune"} {
		for _, purpose := range []agentpb.BackupSecretSlotPurpose{
			agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY,
			agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_OPERATOR_OLD_AGE_IDENTITY,
		} {
			t.Run(stepKind+"/"+purpose.String(), func(t *testing.T) {
				task, plan := controllerBackupSecretTask(t)
				step := backupSecretPurposeCaptureStep(&agentpb.BackupEncryptionAuthority{
					Kind: agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE,
				})
				if stepKind == "prune" {
					step = plan.GetSteps()[0]
				}
				accessSource := []byte("access")
				secretSource := []byte("secret")
				identitySource := []byte("identity")
				resolver := &fakeBackupSecretResolver{
					slots: map[agentpb.BackupSecretSlotPurpose][]byte{
						agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY: accessSource,
						agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY: secretSource,
						purpose: identitySource,
					},
				}
				server := NewWithPrivateTransfers(
					authorizedAuthenticator(),
					NewRegistry(),
					nil,
					&fakePlanResolver{plan: plan},
					nil,
					resolver,
				)
				stream := &snapshotBackupSecretStream{}
				if err := server.sendBackupSecretSlots(
					context.Background(),
					stream,
					backupsecret.Request{
						TaskID: task.ID, AssignmentID: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
						AgentID: "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV", AgentGeneration: 1,
						StepID: step.GetStepId(), Plan: plan, Step: step,
					},
				); err == nil {
					t.Fatal("sendBackupSecretSlots() error = nil")
				}
				if len(stream.sent) != 0 {
					t.Fatalf("sent messages = %#v, want none", stream.sent)
				}
				if !allZero(accessSource) || !allZero(secretSource) || !allZero(identitySource) {
					t.Fatal("rejected purpose set retained plaintext")
				}
				if len(resolver.slots) != 0 {
					t.Fatalf("resolver retained slots = %#v", resolver.slots)
				}
			})
		}
	}
}

type fakeBackupSecretResolver struct {
	slots    map[agentpb.BackupSecretSlotPurpose][]byte
	requests []backupsecret.Request
}

type sequentialBackupSecretResolver struct {
	slotSets []map[agentpb.BackupSecretSlotPurpose][]byte
	requests []backupsecret.Request
}

func (resolver *sequentialBackupSecretResolver) ResolveBackupSecretSlots(
	_ context.Context,
	request backupsecret.Request,
) (map[agentpb.BackupSecretSlotPurpose][]byte, error) {
	resolver.requests = append(resolver.requests, request)
	if len(resolver.slotSets) == 0 {
		return nil, errs.New(errs.KindInternal, "no backup secret slot set remains")
	}
	slots := resolver.slotSets[0]
	resolver.slotSets = resolver.slotSets[1:]
	return slots, nil
}

func (resolver *sequentialBackupSecretResolver) ReleaseRestoreIdentity(context.Context, string) error {
	return nil
}

func (resolver *fakeBackupSecretResolver) ResolveBackupSecretSlots(
	_ context.Context,
	request backupsecret.Request,
) (map[agentpb.BackupSecretSlotPurpose][]byte, error) {
	resolver.requests = append(resolver.requests, request)
	return resolver.slots, nil
}

func (resolver *fakeBackupSecretResolver) ReleaseRestoreIdentity(context.Context, string) error {
	return nil
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
		return errs.New(errs.KindInternal, "injected backup secret send failure")
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
	step := controllerBackupPruneSecretStep(
		t, stepID, "01ARZ3NDEKTSV4RRFFQ69G5FAV", 1,
		"rp_01ARZ3NDEKTSV4RRFFQ69G5FAV", "spt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"con_01ARZ3NDEKTSV4RRFFQ69G5FAV", "https://objects.example.test",
		"groundplane-backups", "production/", true,
	)
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: planID,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE, TargetId: environmentID,
		BackupScope: controllerBackupSecretScope(), Steps: []*agentpb.ExecutionStep{step},
	})
	if err != nil {
		t.Fatalf("seal backup plan: %v", err)
	}
	return etcd.TaskRecord{
		ID:                taskID,
		OperationID:       operationID,
		PlanID:            planID,
		PlanHash:          hex.EncodeToString(plan.PlanHash),
		Type:              testtaskjournal.TaskBackupPrune,
		Target:            environmentID,
		Steps:             []testtaskjournal.TaskStepRecord{{Kind: testtaskjournal.TaskStepOperation, ID: stepID}},
		TimeoutSeconds:    120,
		Status:            testtaskjournal.TaskStatusRunning,
		NextEventSequence: 1,
		CreatedAt:         time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC),
	}, plan
}

func controllerBackupPruneSecretTask(t *testing.T) (etcd.TaskRecord, *agentpb.ExecutionPlan) {
	t.Helper()
	const (
		taskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAX"
		operationID   = "op_01ARZ3NDEKTSV4RRFFQ69G5FAX"
		planID        = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAX"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: planID,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE, TargetId: environmentID,
		BackupScope: controllerBackupSecretScope(),
		Steps: []*agentpb.ExecutionStep{
			controllerBackupPruneSecretStep(
				t, "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", "01ARZ3NDEKTSV4RRFFQ69G5FAV", 1,
				"rp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				"spt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				"con_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				"https://objects.example.test",
				"groundplane-backups",
				"production/",
				true,
			),
			controllerBackupPruneSecretStep(
				t, "step_01ARZ3NDEKTSV4RRFFQ69G5FAW", "01ARZ3NDEKTSV4RRFFQ69G5FAW", 2,
				"rp_01ARZ3NDEKTSV4RRFFQ69G5FAW",
				"spt_01ARZ3NDEKTSV4RRFFQ69G5FAW",
				"con_01ARZ3NDEKTSV4RRFFQ69G5FAW",
				"https://archive.example.test",
				"groundplane-archive",
				"archive/",
				false,
			),
		},
	})
	if err != nil {
		t.Fatalf("seal backup prune plan: %v", err)
	}
	return etcd.TaskRecord{
		ID:          taskID,
		OperationID: operationID,
		PlanID:      planID,
		PlanHash:    hex.EncodeToString(plan.PlanHash),
		Type:        testtaskjournal.TaskBackupPrune,
		Target:      environmentID,
		Steps: []testtaskjournal.TaskStepRecord{
			{Kind: testtaskjournal.TaskStepOperation, ID: plan.Steps[0].StepId},
			{Kind: testtaskjournal.TaskStepOperation, ID: plan.Steps[1].StepId},
		},
		TimeoutSeconds:    120,
		Status:            testtaskjournal.TaskStatusRunning,
		NextEventSequence: 1,
		CreatedAt:         time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC),
	}, plan
}

func controllerBackupPruneSecretStep(
	t *testing.T,
	stepID string,
	executionID string,
	ordinal uint32,
	pointID string,
	sourceID string,
	connectorID string,
	endpoint string,
	bucket string,
	prefix string,
	pathStyle bool,
) *agentpb.ExecutionStep {
	t.Helper()
	digest := bytes.Repeat([]byte{byte(ordinal)}, sha256.Size)
	authority, err := executionplan.SealBackupStepAuthority(&agentpb.BackupStepAuthority{
		StepId: stepID, ExecutionId: executionID,
		StepDeadlineUnixNano: uint64(time.Date(2026, time.August, 24, 12, 30, 0, 0, time.UTC).UnixNano()),
		Operation: &agentpb.BackupStepAuthority_Prune{Prune: &agentpb.BackupPruneAuthority{
			RetentionPolicy: controllerBackupSecretRevision(20, 20),
			Objects: []*agentpb.BackupPruneObject{{
				Ordinal: ordinal, PointId: pointID,
				Point: controllerBackupSecretRevision(int64(30+ordinal), byte(30+ordinal)),
				Evidence: &agentpb.BackupArtifactEvidence{
					SourceSizeBytes: 4096, SourceSha256: append([]byte(nil), digest...),
					StoredSizeBytes: 4096, StoredSha256: append([]byte(nil), digest...),
				},
				Object: &agentpb.BackupObjectIdentity{
					Connector: &agentpb.BackupConnectorAuthority{
						ConnectorId: connectorID, Connector: controllerBackupSecretRevision(40, 40),
						CanonicalEndpointUrl: endpoint, Region: "auto", PathStyle: &pathStyle, Prefix: prefix,
						AccessKeySlotId: backupsecret.AccessKeySlotID, SecretKeySlotId: backupsecret.SecretKeySlotID,
						AccessKeySlot: controllerBackupSecretRevision(41, 41),
						SecretKeySlot: controllerBackupSecretRevision(42, 42),
					},
					Bucket:        bucket,
					ObjectKey:     prefix + "env_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + sourceID + "/" + pointID + "/artifact.bin",
					Discriminator: &agentpb.BackupObjectIdentity_Etag{Etag: &agentpb.BackupS3ETag{Value: "etag"}},
				},
				MetadataCount: 10, MetadataSha256: bytes.Repeat([]byte{50}, sha256.Size),
			}},
		}},
	})
	if err != nil {
		t.Fatalf("SealBackupStepAuthority() error = %v", err)
	}
	return &agentpb.ExecutionStep{
		StepId:         stepID,
		TimeoutSeconds: executionplan.MaximumBackupPruneStepTimeoutSeconds,
		Payload:        &agentpb.ExecutionStep_BackupStep{BackupStep: authority},
	}
}

func controllerBackupSecretScope() *agentpb.BackupPlanScope {
	return &agentpb.BackupPlanScope{
		ProjectId: "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV", Project: controllerBackupSecretRevision(1, 1),
		EnvironmentId: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", Environment: controllerBackupSecretRevision(2, 2),
		TaskAttempt: 1,
	}
}

func controllerBackupSecretRevision(revision int64, fill byte) *agentpb.RevisionDigest {
	return &agentpb.RevisionDigest{ModRevision: revision, Sha256: bytes.Repeat([]byte{fill}, sha256.Size)}
}

func backupSecretPurposeCaptureStep(encryption *agentpb.BackupEncryptionAuthority) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Payload: &agentpb.ExecutionStep_BackupStep{BackupStep: &agentpb.BackupStepAuthority{
			Operation: &agentpb.BackupStepAuthority_Capture{Capture: &agentpb.BackupCaptureAuthority{
				Encryption: encryption,
			}},
		}},
	}
}

func allZero(value []byte) bool {
	for _, current := range value {
		if current != 0 {
			return false
		}
	}
	return true
}
