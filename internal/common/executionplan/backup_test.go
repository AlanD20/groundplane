package executionplan

import (
	"bytes"
	"testing"

	"filippo.io/age"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	testBackupEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupConnectorID   = "con_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: the private machine plan must close all three MVP source kinds
// over stable identities and immutable captured revisions without Compose
// artifacts or an invented render generation.
func TestSealAndValidateBackupSourcePlan(t *testing.T) {
	plan := validBackupPlan(t)
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal(Backup) error = %v", err)
	}
	if _, err := Validate(sealed); err != nil {
		t.Fatalf("Validate(Backup) error = %v", err)
	}
}

// Rationale: format and typed source are one closed pair; accepting a
// mismatched pair could authorize the wrong compiled capture implementation.
func TestSealRejectsMismatchedBackupSourceFormat(t *testing.T) {
	plan := validBackupPlan(t)
	plan.Steps[0].GetBackupSourceCapture().SourceFormat =
		agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_VOLUME_TAR_V1
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(mismatched Backup source format) error = nil")
	}
}

// Rationale: Volume quiescing must restore one unambiguous pinned service
// table, so duplicate or unsorted service ids cannot enter a sealed plan.
func TestSealRejectsUnsortedBackupVolumeServices(t *testing.T) {
	plan := validBackupPlan(t)
	volume := plan.Steps[2].GetBackupSourceCapture().GetVolume()
	volume.Services = append(volume.Services, volume.Services[0])
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(duplicate Volume Service) error = nil")
	}
}

// Rationale: config is the Environment-wide source and cannot name another
// Environment even when that target id is otherwise canonical.
func TestSealRejectsBackupConfigTargetOutsidePlanEnvironment(t *testing.T) {
	plan := validBackupPlan(t)
	plan.Steps[1].GetBackupSourceCapture().TargetId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(config target outside plan Environment) error = nil")
	}
}

// Rationale: every source comes from one policy-revision snapshot, whose
// Connector and encryption/key-era decision are Environment-wide.
func TestSealRejectsMixedBackupPolicyControls(t *testing.T) {
	plan := validBackupPlan(t)
	plan.Steps[2].GetBackupSourceCapture().ConnectorRevision++
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(mixed policy controls) error = nil")
	}
}

// Rationale: recipients are cryptographic control values, not arbitrary
// bounded strings; only the canonical X25519 recipient spelling is accepted.
func TestSealRejectsNonCanonicalAgeRecipient(t *testing.T) {
	plan := validBackupPlan(t)
	plan.Steps[0].GetBackupSourceCapture().AgeRecipient = " AGE1NOTCANONICAL "
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(non-canonical age recipient) error = nil")
	}
}

// Rationale: checkpoint acknowledgements are deliberately incapable of
// smuggling result data: they echo only the exact assignment delivery tuple.
func TestBackupCheckpointDescriptorClosesAssignmentIdentity(t *testing.T) {
	request := (&agentpb.BackupCheckpointRequest{}).ProtoReflect().Descriptor()
	for _, name := range []protoreflect.Name{
		"task_id", "assignment_id", "step_id", "sequence", "kind", "control_payload_sha256",
	} {
		if request.Fields().ByName(name) == nil {
			t.Fatalf("BackupCheckpointRequest missing %q", name)
		}
	}
	if request.Oneofs().ByName("payload") == nil || request.Oneofs().ByName("payload").Fields().Len() != 11 {
		t.Fatalf("BackupCheckpointRequest payload variants = %v", request.Oneofs().ByName("payload"))
	}
	ack := (&agentpb.BackupCheckpointAck{}).ProtoReflect().Descriptor()
	if ack.Fields().Len() != 4 {
		t.Fatalf("BackupCheckpointAck fields = %d, want exact identity-only 4", ack.Fields().Len())
	}
	for _, name := range []protoreflect.Name{"task_id", "assignment_id", "step_id", "sequence"} {
		if ack.Fields().ByName(name) == nil {
			t.Fatalf("BackupCheckpointAck missing %q", name)
		}
	}
}

// Rationale: the next-sequence cursor, assignment tuple, closed kind/payload
// pair, and digest shapes must all validate before a durable commit can use a
// private checkpoint delivery.
func TestValidateBackupCheckpointRequestAndAck(t *testing.T) {
	request := validBackupCheckpointRequest()
	validated, err := ValidateBackupCheckpointRequest(request, 1)
	if err != nil {
		t.Fatalf("ValidateBackupCheckpointRequest() error = %v", err)
	}
	validated.ControlPayloadSha256[0]++
	if bytes.Equal(validated.ControlPayloadSha256, request.ControlPayloadSha256) {
		t.Fatal("validated checkpoint aliases request digest")
	}
	ack := &agentpb.BackupCheckpointAck{
		TaskId: request.TaskId, AssignmentId: request.AssignmentId,
		StepId: request.StepId, Sequence: request.Sequence,
	}
	if _, err := ValidateBackupCheckpointAck(ack, request); err != nil {
		t.Fatalf("ValidateBackupCheckpointAck() error = %v", err)
	}
}

// Rationale: no stale sequence, malformed digest, or kind/payload mismatch may
// reach the future checkpoint transaction classifier.
func TestValidateBackupCheckpointRequestRejectsMalformedDelivery(t *testing.T) {
	checks := []struct {
		name   string
		mutate func(*agentpb.BackupCheckpointRequest)
	}{
		{name: "zero sequence", mutate: func(request *agentpb.BackupCheckpointRequest) { request.Sequence = 0 }},
		{name: "short control digest", mutate: func(request *agentpb.BackupCheckpointRequest) {
			request.ControlPayloadSha256 = request.ControlPayloadSha256[:31]
		}},
		{name: "kind mismatch", mutate: func(request *agentpb.BackupCheckpointRequest) {
			request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_UPLOAD_VERIFIED
		}},
		{name: "bad point", mutate: func(request *agentpb.BackupCheckpointRequest) {
			request.GetArtifactPrepared().PointId = "rp_invalid"
		}},
		{name: "short artifact digest", mutate: func(request *agentpb.BackupCheckpointRequest) {
			request.GetArtifactPrepared().StoredSha256 = request.GetArtifactPrepared().StoredSha256[:31]
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			request := validBackupCheckpointRequest()
			check.mutate(request)
			if _, err := ValidateBackupCheckpointRequest(request, 1); err == nil {
				t.Fatal("ValidateBackupCheckpointRequest(malformed) error = nil")
			}
		})
	}
	request := validBackupCheckpointRequest()
	if _, err := ValidateBackupCheckpointRequest(request, 2); err == nil {
		t.Fatal("ValidateBackupCheckpointRequest(stale sequence) error = nil")
	}
}

// Rationale: the acknowledgement is useful only for the exact request; a
// different assignment or sequence cannot release the Agent's next action.
func TestValidateBackupCheckpointAckRejectsDifferentIdentity(t *testing.T) {
	request := validBackupCheckpointRequest()
	ack := &agentpb.BackupCheckpointAck{
		TaskId: request.TaskId, AssignmentId: request.AssignmentId,
		StepId: request.StepId, Sequence: request.Sequence + 1,
	}
	if _, err := ValidateBackupCheckpointAck(ack, request); err == nil {
		t.Fatal("ValidateBackupCheckpointAck(different sequence) error = nil")
	}
}

func validBackupPlan(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("GenerateX25519Identity() error = %v", err)
	}
	recipient := identity.Recipient().String()
	return &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: testPlanID,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP, TargetId: testBackupEnvironmentID,
		Steps: []*agentpb.ExecutionStep{
			backupStep("step_01ARZ3NDEKTSV4RRFFQ69G5FAV", &agentpb.BackupSourceCapture{
				SourceId: "spt_01ARZ3NDEKTSV4RRFFQ69G5FAV", SourceRevision: 11,
				TargetId: "att_01ARZ3NDEKTSV4RRFFQ69G5FAV", TargetRevision: 12,
				PointId: "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV", ConnectorId: testBackupConnectorID,
				ConnectorRevision: 13,
				SourceFormat:      agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_POSTGRES_CUSTOM_V1,
				Encryption:        agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE, KeyEra: 2,
				AgeRecipient: recipient,
				Source: &agentpb.BackupSourceCapture_Attach{Attach: &agentpb.BackupAttachSource{
					BackingServiceId: "bks_01ARZ3NDEKTSV4RRFFQ69G5FAV", BackingServiceRevision: 14,
					Database: "application", Role: "application_owner",
				}},
			}),
			backupStep("step_01ARZ3NDEKTSV4RRFFQ69G5FAW", &agentpb.BackupSourceCapture{
				SourceId: "spt_01ARZ3NDEKTSV4RRFFQ69G5FAW", SourceRevision: 21,
				TargetId: testBackupEnvironmentID, TargetRevision: 22,
				PointId: "rp_01ARZ3NDEKTSV4RRFFQ69G5FAW", ConnectorId: testBackupConnectorID,
				ConnectorRevision: 13,
				SourceFormat:      agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_ENVIRONMENT_CONFIG_V1,
				Encryption:        agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE, KeyEra: 2,
				AgeRecipient: recipient,
				Source: &agentpb.BackupSourceCapture_Config{Config: &agentpb.BackupConfigSource{
					SnapshotRevision: 24,
				}},
			}),
			backupStep("step_01ARZ3NDEKTSV4RRFFQ69G5FAX", &agentpb.BackupSourceCapture{
				SourceId: "spt_01ARZ3NDEKTSV4RRFFQ69G5FAX", SourceRevision: 31,
				TargetId: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV", TargetRevision: 32,
				PointId: "rp_01ARZ3NDEKTSV4RRFFQ69G5FAX", ConnectorId: testBackupConnectorID,
				ConnectorRevision: 13,
				SourceFormat:      agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_VOLUME_TAR_V1,
				Encryption:        agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE, KeyEra: 2,
				AgeRecipient: recipient,
				Source: &agentpb.BackupSourceCapture_Volume{Volume: &agentpb.BackupVolumeSource{
					Services: []*agentpb.BackupVolumeService{{
						ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceRevision: 34,
						PriorIntent: agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING,
					}},
				}},
			}),
		},
	}
}

func validBackupCheckpointRequest() *agentpb.BackupCheckpointRequest {
	return &agentpb.BackupCheckpointRequest{
		TaskId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", Sequence: 1,
		Kind:                 agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_ARTIFACT_PREPARED,
		ControlPayloadSha256: bytes.Repeat([]byte{1}, 32),
		Payload: &agentpb.BackupCheckpointRequest_ArtifactPrepared{
			ArtifactPrepared: &agentpb.BackupArtifactPreparedCheckpoint{
				PointId: "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV", StoredSizeBytes: 4096,
				StoredSha256: bytes.Repeat([]byte{2}, 32),
			},
		},
	}
}

func backupStep(stepID string, capture *agentpb.BackupSourceCapture) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: 600,
		Payload: &agentpb.ExecutionStep_BackupSourceCapture{BackupSourceCapture: capture},
	}
}
