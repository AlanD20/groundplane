package executionplan

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
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

// Rationale: one internal prune Task may delete ordered points from different
// Connectors, so each step must remain independently executable under its own
// assignment/step-fenced S3 credential slots.
func TestSealAndValidateBackupPrunePlanAcrossConnectors(t *testing.T) {
	plan := validBackupPrunePlan()
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal(Backup prune) error = %v", err)
	}
	if _, err := Validate(sealed); err != nil {
		t.Fatalf("Validate(Backup prune) error = %v", err)
	}
	first := sealed.Steps[0].GetBackupArtifactPrune()
	second := sealed.Steps[1].GetBackupArtifactPrune()
	if first.ConnectorId == second.ConnectorId {
		t.Fatal("backup prune fixture did not exercise different Connectors")
	}
}

// Rationale: the private prune step is a closed control payload. Credentials,
// age identities, provider responses, and generic parameter maps must not gain
// a field in the machine contract.
func TestBackupArtifactPruneDescriptorIsClosed(t *testing.T) {
	descriptor := (&agentpb.BackupArtifactPrune{}).ProtoReflect().Descriptor()
	want := []protoreflect.Name{
		"ordinal", "prune_operation_id", "prune_revision", "point_id", "point_revision",
		"source_id", "source_revision", "environment_id", "environment_revision",
		"connector_id", "connector_revision", "connector_endpoint", "connector_bucket",
		"connector_prefix", "connector_region", "connector_addressing", "protected_object_key",
		"stored_size_bytes", "stored_sha256",
	}
	if descriptor.Fields().Len() != len(want) {
		t.Fatalf("BackupArtifactPrune fields = %d, want %d", descriptor.Fields().Len(), len(want))
	}
	for _, name := range want {
		if descriptor.Fields().ByName(name) == nil {
			t.Fatalf("BackupArtifactPrune missing %q", name)
		}
	}
}

// Rationale: prune ordering, point uniqueness, immutable revisions, Connector
// decisions, exact object evidence, and the 30-minute per-point budget must all
// fail closed before an Agent can receive the plan.
func TestSealRejectsMalformedBackupPrunePlan(t *testing.T) {
	checks := []struct {
		name   string
		mutate func(*agentpb.ExecutionPlan)
	}{
		{name: "no steps", mutate: func(plan *agentpb.ExecutionPlan) { plan.Steps = nil }},
		{name: "too many steps", mutate: func(plan *agentpb.ExecutionPlan) {
			for len(plan.Steps) <= MaximumBackupPrunePoints {
				plan.Steps = append(plan.Steps, plan.Steps[0])
			}
		}},
		{name: "render generation", mutate: func(plan *agentpb.ExecutionPlan) { plan.RenderGeneration = 1 }},
		{name: "wrong target", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.TargetId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		}},
		{name: "wrong step timeout", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].TimeoutSeconds = MaximumBackupPruneStepTimeoutSeconds - 1
		}},
		{name: "ordinal gap", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Steps[1].GetBackupArtifactPrune().Ordinal = 3
		}},
		{name: "duplicate point", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Steps[1].GetBackupArtifactPrune().PointId = plan.Steps[0].GetBackupArtifactPrune().PointId
		}},
		{name: "different prune operation", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Steps[1].GetBackupArtifactPrune().PruneOperationId = "op_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		}},
		{name: "zero prune revision", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupArtifactPrune().PruneRevision = 0
		}},
		{name: "bad endpoint", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupArtifactPrune().ConnectorEndpoint += "?query=forbidden"
		}},
		{name: "bad bucket", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupArtifactPrune().ConnectorBucket = "INVALID"
		}},
		{name: "bad prefix", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupArtifactPrune().ConnectorPrefix = "not-normalized"
		}},
		{name: "empty region", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupArtifactPrune().ConnectorRegion = ""
		}},
		{name: "unspecified addressing", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupArtifactPrune().ConnectorAddressing =
				agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_UNSPECIFIED
		}},
		{name: "wrong protected object key", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupArtifactPrune().ProtectedObjectKey = "another/object"
		}},
		{name: "zero stored size", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupArtifactPrune().StoredSizeBytes = 0
		}},
		{name: "short stored digest", mutate: func(plan *agentpb.ExecutionPlan) {
			prune := plan.Steps[0].GetBackupArtifactPrune()
			prune.StoredSha256 = prune.StoredSha256[:31]
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			plan := validBackupPrunePlan()
			check.mutate(plan)
			if _, err := Seal(plan); err == nil {
				t.Fatal("Seal(malformed Backup prune) error = nil")
			}
		})
	}
}

// Rationale: Backup capture and prune are different closed procedures. A plan
// cannot change the operation enum while retaining the other operation's step.
func TestSealRejectsBackupAndPruneStepOperationMismatch(t *testing.T) {
	prune := validBackupPrunePlan()
	prune.Operation = agentpb.PlanOperation_PLAN_OPERATION_BACKUP
	if _, err := Seal(prune); err == nil {
		t.Fatal("Seal(Backup operation with prune step) error = nil")
	}
	backup := validBackupPlan(t)
	backup.Operation = agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE
	if _, err := Seal(backup); err == nil {
		t.Fatal("Seal(Backup prune operation with capture step) error = nil")
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
	if request.Oneofs().ByName("payload") == nil || request.Oneofs().ByName("payload").Fields().Len() != 12 {
		t.Fatalf("BackupCheckpointRequest payload variants = %v", request.Oneofs().ByName("payload"))
	}
	if request.Fields().ByName("upload_completed") == nil {
		t.Fatal("BackupCheckpointRequest missing upload_completed")
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

// Rationale: artifact-prepared is the durable pre-Put intent, but the private
// checkpoint carries only the point and immutable byte evidence. Object
// identity stays pinned in the run record rather than entering Agent traffic.
func TestBackupArtifactPreparedCheckpointClosesPrePutIntentEvidence(t *testing.T) {
	descriptor := (&agentpb.BackupArtifactPreparedCheckpoint{}).ProtoReflect().Descriptor()
	if descriptor.Fields().Len() != 3 {
		t.Fatalf("BackupArtifactPreparedCheckpoint fields = %d, want 3", descriptor.Fields().Len())
	}
	for _, name := range []protoreflect.Name{"point_id", "stored_size_bytes", "stored_sha256"} {
		if descriptor.Fields().ByName(name) == nil {
			t.Fatalf("BackupArtifactPreparedCheckpoint missing %q", name)
		}
	}
	request := validBackupCheckpointRequest()
	if _, err := ValidateBackupCheckpointRequest(request, request.Sequence); err != nil {
		t.Fatalf("ValidateBackupCheckpointRequest(artifact prepared) error = %v", err)
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

// Rationale: a config restore generation is a stable Controller-owned config
// identity distinct from the Task attempt carried by the checkpoint envelope.
func TestValidateBackupConfigGenerationCheckpointsRequireConfigIdentity(t *testing.T) {
	requests := []*agentpb.BackupCheckpointRequest{
		validBackupConfigGenerationStagedCheckpointRequest(),
		validBackupConfigGenerationActivatedCheckpointRequest(),
	}
	for _, request := range requests {
		digest, err := ComputeBackupCheckpointPayloadDigest(request)
		if err != nil {
			t.Fatalf("ComputeBackupCheckpointPayloadDigest(config generation) error = %v", err)
		}
		request.ControlPayloadSha256 = digest
		if _, err := ValidateBackupCheckpointRequest(request, request.Sequence); err != nil {
			t.Fatalf("ValidateBackupCheckpointRequest(config generation) error = %v", err)
		}

		switch payload := request.Payload.(type) {
		case *agentpb.BackupCheckpointRequest_ConfigGenerationStaged:
			payload.ConfigGenerationStaged.RestoreGenerationId = request.TaskId
		case *agentpb.BackupCheckpointRequest_ConfigGenerationActivated:
			payload.ConfigGenerationActivated.RestoreGenerationId = request.TaskId
		default:
			t.Fatalf("unexpected config generation payload %T", request.Payload)
		}
		if _, err := ComputeBackupCheckpointPayloadDigest(request); err == nil {
			t.Fatal("Task identity accepted as restore generation")
		}
	}
}

// Rationale: the durable deduplication digest must remain independent of
// protobuf wire serialization and assignment-envelope changes.
func TestComputeBackupCheckpointPayloadDigestUsesVersionOneGrammar(t *testing.T) {
	request := validBackupCheckpointRequest()
	digest, err := ComputeBackupCheckpointPayloadDigest(request)
	if err != nil {
		t.Fatalf("ComputeBackupCheckpointPayloadDigest() error = %v", err)
	}
	var encoded bytes.Buffer
	encoded.WriteString("groundplane.backup.checkpoint.v1")
	encoded.WriteByte(0)
	if err := binary.Write(
		&encoded,
		binary.BigEndian,
		uint32(agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_ARTIFACT_PREPARED),
	); err != nil {
		t.Fatalf("encode kind: %v", err)
	}
	pointID := request.GetArtifactPrepared().PointId
	if err := binary.Write(&encoded, binary.BigEndian, uint32(len(pointID))); err != nil {
		t.Fatalf("encode point length: %v", err)
	}
	encoded.WriteString(pointID)
	if err := binary.Write(&encoded, binary.BigEndian, request.GetArtifactPrepared().StoredSizeBytes); err != nil {
		t.Fatalf("encode stored size: %v", err)
	}
	encoded.Write(request.GetArtifactPrepared().StoredSha256)
	want := sha256.Sum256(encoded.Bytes())
	if !bytes.Equal(digest, want[:]) {
		t.Fatalf("checkpoint digest = %x, want %x", digest, want)
	}
}

// Rationale: Put success is durable orphan evidence before Head starts. Its
// digest must use the existing protobuf-independent v1 grammar without
// changing any previously assigned checkpoint kind.
func TestComputeBackupCheckpointPayloadDigestUploadCompletedUsesVersionOneGrammar(t *testing.T) {
	request := validUploadCompletedCheckpointRequest()
	digest, err := ComputeBackupCheckpointPayloadDigest(request)
	if err != nil {
		t.Fatalf("ComputeBackupCheckpointPayloadDigest(upload completed) error = %v", err)
	}
	var encoded bytes.Buffer
	encoded.WriteString("groundplane.backup.checkpoint.v1")
	encoded.WriteByte(0)
	if err := binary.Write(
		&encoded,
		binary.BigEndian,
		uint32(agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_UPLOAD_COMPLETED),
	); err != nil {
		t.Fatalf("encode kind: %v", err)
	}
	pointID := request.GetUploadCompleted().PointId
	if err := binary.Write(&encoded, binary.BigEndian, uint32(len(pointID))); err != nil {
		t.Fatalf("encode point length: %v", err)
	}
	encoded.WriteString(pointID)
	if err := binary.Write(&encoded, binary.BigEndian, request.GetUploadCompleted().StoredSizeBytes); err != nil {
		t.Fatalf("encode stored size: %v", err)
	}
	encoded.Write(request.GetUploadCompleted().StoredSha256)
	want := sha256.Sum256(encoded.Bytes())
	if !bytes.Equal(digest, want[:]) {
		t.Fatalf("upload-completed checkpoint digest = %x, want %x", digest, want)
	}
	request.ControlPayloadSha256 = digest
	if _, err := ValidateBackupCheckpointRequest(request, request.Sequence); err != nil {
		t.Fatalf("ValidateBackupCheckpointRequest(upload completed) error = %v", err)
	}
}

// Rationale: upload-completed is useful as orphan evidence only when it names
// one canonical point and exact non-empty immutable-object size/digest facts.
func TestValidateBackupCheckpointRequestRejectsMalformedUploadCompleted(t *testing.T) {
	checks := []struct {
		name   string
		mutate func(*agentpb.BackupCheckpointRequest)
	}{
		{name: "kind mismatch", mutate: func(request *agentpb.BackupCheckpointRequest) {
			request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_UPLOAD_VERIFIED
		}},
		{name: "bad point", mutate: func(request *agentpb.BackupCheckpointRequest) {
			request.GetUploadCompleted().PointId = "rp_invalid"
		}},
		{name: "zero stored size", mutate: func(request *agentpb.BackupCheckpointRequest) {
			request.GetUploadCompleted().StoredSizeBytes = 0
		}},
		{name: "short stored digest", mutate: func(request *agentpb.BackupCheckpointRequest) {
			request.GetUploadCompleted().StoredSha256 = request.GetUploadCompleted().StoredSha256[:31]
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			request := validUploadCompletedCheckpointRequest()
			check.mutate(request)
			digest, err := ComputeBackupCheckpointPayloadDigest(request)
			if err == nil {
				request.ControlPayloadSha256 = digest
				_, err = ValidateBackupCheckpointRequest(request, request.Sequence)
			}
			if err == nil {
				t.Fatal("malformed upload-completed checkpoint error = nil")
			}
		})
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
				Upload: testBackupUpload(
					"spt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
					"rp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				),
				Source: &agentpb.BackupSourceCapture_Attach{Attach: &agentpb.BackupAttachSource{
					BackingServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", BackingServiceRevision: 14,
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
				Upload: testBackupUpload(
					"spt_01ARZ3NDEKTSV4RRFFQ69G5FAW",
					"rp_01ARZ3NDEKTSV4RRFFQ69G5FAW",
				),
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
				Upload: testBackupUpload(
					"spt_01ARZ3NDEKTSV4RRFFQ69G5FAX",
					"rp_01ARZ3NDEKTSV4RRFFQ69G5FAX",
				),
				Source: &agentpb.BackupSourceCapture_Volume{Volume: &agentpb.BackupVolumeSource{
					ArtifactId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV", ArtifactSha256: bytes.Repeat([]byte{1}, 32),
					ArtifactRevision: 35, ProjectionRoot: 36, RenderGeneration: 1,
					ComposeVolumeKey: "data", DockerVolumeName: "gp_vol_vol_01ARZ3NDEKTSV4RRFFQ69G5FAV",
					AuthorizedVolumeDir: "/var/lib/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
						"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + testBackupEnvironmentID,
					Services: []*agentpb.BackupVolumeService{{
						ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceRevision: 34,
						PriorIntent: agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING,
						ComposeKey:  "application", MountPaths: []string{"/var/lib/application"},
					}},
				}},
			}),
		},
	}
}

func testBackupUpload(sourceID string, pointID string) *agentpb.BackupUploadAuthority {
	const prefix = "production/"
	return &agentpb.BackupUploadAuthority{
		ConnectorEndpoint: "https://objects.example.test", ConnectorBucket: "groundplane-backups",
		ConnectorPrefix: prefix, ConnectorRegion: "auto",
		ConnectorAddressing: agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_PATH_STYLE,
		ProtectedObjectKey:  prefix + testBackupEnvironmentID + "/" + sourceID + "/" + pointID + "/artifact.bin",
		ImmutableCreate:     true, PutAfterArtifactPreparedAck: true, HeadAfterUploadCompletedAck: true,
	}
}

func validBackupCheckpointRequest() *agentpb.BackupCheckpointRequest {
	request := &agentpb.BackupCheckpointRequest{
		TaskId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", Sequence: 1,
		Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_ARTIFACT_PREPARED,
		Payload: &agentpb.BackupCheckpointRequest_ArtifactPrepared{
			ArtifactPrepared: &agentpb.BackupArtifactPreparedCheckpoint{
				PointId: "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV", StoredSizeBytes: 4096,
				StoredSha256: bytes.Repeat([]byte{2}, 32),
			},
		},
	}
	digest, err := ComputeBackupCheckpointPayloadDigest(request)
	if err != nil {
		panic(err)
	}
	request.ControlPayloadSha256 = digest
	return request
}

func validUploadCompletedCheckpointRequest() *agentpb.BackupCheckpointRequest {
	return &agentpb.BackupCheckpointRequest{
		TaskId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", Sequence: 2,
		Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_UPLOAD_COMPLETED,
		Payload: &agentpb.BackupCheckpointRequest_UploadCompleted{
			UploadCompleted: &agentpb.BackupUploadCompletedCheckpoint{
				PointId: "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV", StoredSizeBytes: 4096,
				StoredSha256: bytes.Repeat([]byte{3}, 32),
			},
		},
	}
}

func validBackupPrunePlan() *agentpb.ExecutionPlan {
	const pruneOperationID = "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	return &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE, TargetId: testBackupEnvironmentID,
		Steps: []*agentpb.ExecutionStep{
			backupPruneStep(
				"step_01ARZ3NDEKTSV4RRFFQ69G5FAV", 1, pruneOperationID,
				"rp_01ARZ3NDEKTSV4RRFFQ69G5FAV", "spt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				testBackupConnectorID, "https://objects.example.test", "groundplane-backups", "production/",
				agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_PATH_STYLE,
			),
			backupPruneStep(
				"step_01ARZ3NDEKTSV4RRFFQ69G5FAW", 2, pruneOperationID,
				"rp_01ARZ3NDEKTSV4RRFFQ69G5FAW", "spt_01ARZ3NDEKTSV4RRFFQ69G5FAW",
				"con_01ARZ3NDEKTSV4RRFFQ69G5FAW", "https://archive.example.test", "groundplane-archive",
				"archive/", agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_VIRTUAL_HOSTED_STYLE,
			),
		},
	}
}

func backupPruneStep(
	stepID string,
	ordinal uint32,
	operationID string,
	pointID string,
	sourceID string,
	connectorID string,
	endpoint string,
	bucket string,
	prefix string,
	addressing agentpb.BackupS3Addressing,
) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: MaximumBackupPruneStepTimeoutSeconds,
		Payload: &agentpb.ExecutionStep_BackupArtifactPrune{BackupArtifactPrune: &agentpb.BackupArtifactPrune{
			Ordinal: ordinal, PruneOperationId: operationID, PruneRevision: uint64(100 + ordinal),
			PointId: pointID, PointRevision: uint64(200 + ordinal),
			SourceId: sourceID, SourceRevision: uint64(300 + ordinal),
			EnvironmentId: testBackupEnvironmentID, EnvironmentRevision: 400,
			ConnectorId: connectorID, ConnectorRevision: uint64(500 + ordinal),
			ConnectorEndpoint: endpoint, ConnectorBucket: bucket, ConnectorPrefix: prefix,
			ConnectorRegion: "auto", ConnectorAddressing: addressing,
			ProtectedObjectKey: prefix + testBackupEnvironmentID + "/" + sourceID + "/" + pointID + "/artifact.bin",
			StoredSizeBytes:    4096, StoredSha256: bytes.Repeat([]byte{byte(ordinal)}, sha256.Size),
		}},
	}
}

func validBackupConfigGenerationStagedCheckpointRequest() *agentpb.BackupCheckpointRequest {
	return &agentpb.BackupCheckpointRequest{
		TaskId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", Sequence: 3,
		Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_CONFIG_GENERATION_STAGED,
		Payload: &agentpb.BackupCheckpointRequest_ConfigGenerationStaged{
			ConfigGenerationStaged: &agentpb.BackupConfigGenerationStagedCheckpoint{
				PointId: "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV", RestoreGenerationId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				EntryGenerationManifestSha256: bytes.Repeat([]byte{4}, 32),
			},
		},
	}
}

func validBackupConfigGenerationActivatedCheckpointRequest() *agentpb.BackupCheckpointRequest {
	return &agentpb.BackupCheckpointRequest{
		TaskId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", Sequence: 4,
		Kind: agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_CONFIG_GENERATION_ACTIVATED,
		Payload: &agentpb.BackupCheckpointRequest_ConfigGenerationActivated{
			ConfigGenerationActivated: &agentpb.BackupConfigGenerationActivatedCheckpoint{
				PointId: "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV", RestoreGenerationId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				RenderGeneration: 17,
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
