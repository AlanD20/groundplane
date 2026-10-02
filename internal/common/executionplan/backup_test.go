package executionplan

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	testBackupProjectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupConnectorID   = "con_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupSourceID      = "spt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupPointID       = "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: a private machine plan must preserve the captured Config source,
// immutable revisions, object target, encryption decision and absolute step
// authority without reconstructing any of them at assignment time.
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

// Rationale: the typed source and resource authority are one closed pair;
// accepting Config execution for a Volume resource would authorize the wrong
// compiled capture implementation.
func TestSealRejectsMismatchedBackupSourceResource(t *testing.T) {
	plan := validBackupPlan(t)
	step := plan.Steps[0].GetBackupStep()
	step.GetCapture().Resource.Kind = agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_VOLUME
	resealBackupExecutionStep(t, plan.Steps[0])
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(mismatched Backup source resource) error = nil")
	}
}

// Rationale: one consumer may appear only once in a step authority; duplicate
// identities could otherwise repeat stop/restart effects.
func TestSealRejectsDuplicateBackupConsumers(t *testing.T) {
	plan := validBackupPlan(t)
	serviceID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	plan.BackupScope.Services = []*agentpb.BackupServiceFact{validBackupServiceFactFixture(serviceID)}
	plan.Steps[0].GetBackupStep().ConsumerServiceIds = []string{serviceID, serviceID}
	resealBackupExecutionStep(t, plan.Steps[0])
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(duplicate Backup consumers) error = nil")
	}
}

// Rationale: Config is the Environment-wide source and cannot name another
// Environment even when that target id is otherwise canonical.
func TestSealRejectsBackupConfigTargetOutsidePlanEnvironment(t *testing.T) {
	plan := validBackupPlan(t)
	capture := plan.Steps[0].GetBackupStep().GetCapture()
	capture.Resource.ResourceId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	capture.GetConfig().EnvironmentId = capture.Resource.ResourceId
	resealBackupExecutionStep(t, plan.Steps[0])
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(config target outside plan Environment) error = nil")
	}
}

// Rationale: the public recipient is represented only by its immutable digest;
// a malformed digest cannot enter a sealed age-encrypted capture authority.
func TestSealRejectsMalformedAgeRecipientDigest(t *testing.T) {
	plan := validBackupPlan(t)
	step := plan.Steps[0]
	step.GetBackupStep().GetCapture().Encryption.RecipientSha256 = bytes.Repeat([]byte{1}, sha256.Size-1)
	resealBackupExecutionStep(t, step)
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(malformed age recipient digest) error = nil")
	}
}

// Rationale: Connector and encryption controls are part of the sealed step
// digest; changing a captured policy revision after publication must invalidate
// the plan rather than silently alter the Agent's authority.
func TestSealRejectsMutatedBackupPolicyControls(t *testing.T) {
	plan := validBackupPlan(t)
	plan.Steps[0].GetBackupStep().GetCapture().Target.Connector.Connector.ModRevision++
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(mutated Backup policy controls) error = nil")
	}
}

// Rationale: one internal prune Task may delete ordered points from different
// Connectors, while every object remains independently fenced by its own exact
// Connector authority and immutable discriminator.
func TestSealAndValidateBackupPrunePlanAcrossConnectors(t *testing.T) {
	plan := validBackupPrunePlan(t)
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal(Backup prune) error = %v", err)
	}
	if _, err := Validate(sealed); err != nil {
		t.Fatalf("Validate(Backup prune) error = %v", err)
	}
	first := sealed.Steps[0].GetBackupStep().GetPrune().Objects[0].Object.Connector
	second := sealed.Steps[1].GetBackupStep().GetPrune().Objects[0].Object.Connector
	if first.ConnectorId == second.ConnectorId {
		t.Fatal("backup prune fixture did not exercise different Connectors")
	}
}

// Rationale: prune ordering, point uniqueness, immutable revisions, Connector
// decisions, exact object evidence, and bounded step identity all fail closed
// before an Agent can receive the plan.
func TestSealRejectsMalformedBackupPrunePlan(t *testing.T) {
	checks := []struct {
		name   string
		mutate func(*testing.T, *agentpb.ExecutionPlan)
	}{
		{name: "no steps", mutate: func(_ *testing.T, plan *agentpb.ExecutionPlan) { plan.Steps = nil }},
		{name: "too many steps", mutate: func(_ *testing.T, plan *agentpb.ExecutionPlan) {
			for len(plan.Steps) <= MaximumBackupPrunePoints {
				plan.Steps = append(plan.Steps, plan.Steps[0])
			}
		}},
		{
			name:   "render generation",
			mutate: func(_ *testing.T, plan *agentpb.ExecutionPlan) { plan.RenderGeneration = 1 },
		},
		{name: "wrong target", mutate: func(_ *testing.T, plan *agentpb.ExecutionPlan) {
			plan.TargetId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		}},
		{name: "zero step timeout", mutate: func(_ *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].TimeoutSeconds = 0
		}},
		{name: "duplicate point", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			prune := plan.Steps[0].GetBackupStep().GetPrune()
			duplicate := proto.Clone(prune.Objects[0]).(*agentpb.BackupPruneObject)
			duplicate.Ordinal = 2
			prune.Objects = append(prune.Objects, duplicate)
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "zero retention revision", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().RetentionPolicy.ModRevision = 0
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "zero ordinal", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Ordinal = 0
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "bad endpoint", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Object.Connector.CanonicalEndpointUrl += "?query=forbidden"
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "bad bucket", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Object.Bucket = "INVALID"
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "bad prefix", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Object.Connector.Prefix = "not-normalized"
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "empty region", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Object.Connector.Region = ""
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "missing addressing decision", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Object.Connector.PathStyle = nil
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "wrong object key", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Object.ObjectKey = "another/object"
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "zero stored size", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Evidence.StoredSizeBytes = 0
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "short stored digest", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			object := plan.Steps[0].GetBackupStep().GetPrune().Objects[0]
			object.Evidence.StoredSha256 = object.Evidence.StoredSha256[:sha256.Size-1]
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			plan := validBackupPrunePlan(t)
			check.mutate(t, plan)
			if _, err := Seal(plan); err == nil {
				t.Fatal("Seal(malformed Backup prune) error = nil")
			}
		})
	}
}

// Rationale: Backup capture and prune are different closed procedures. A plan
// cannot change the operation enum while retaining the other operation's step.
func TestSealRejectsBackupAndPruneStepOperationMismatch(t *testing.T) {
	prune := validBackupPrunePlan(t)
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

// Rationale: the validated checkpoint is an owned copy, and the acknowledgement
// can release the Agent only when it carries the exact execution tuple plus a
// durable committed fence for the same authority digest.
func TestValidateBackupCheckpointRequestAndAck(t *testing.T) {
	request := validBackupCheckpointRequest()
	validated, err := ValidateBackupCheckpointRequest(request, request.CheckpointSequence)
	if err != nil {
		t.Fatalf("ValidateBackupCheckpointRequest() error = %v", err)
	}
	validated.AuthorityDigest[0]++
	if bytes.Equal(validated.AuthorityDigest, request.AuthorityDigest) {
		t.Fatal("validated checkpoint aliases request authority digest")
	}
	ack := &agentpb.BackupCheckpointAck{
		TaskId: request.TaskId, AssignmentId: request.AssignmentId,
		StepId: request.StepId, ExecutionId: request.ExecutionId,
		CheckpointSequence: request.CheckpointSequence,
		Committed: &agentpb.CheckpointFence{
			AuthorityDigest: append([]byte(nil), request.AuthorityDigest...), DedupeKeyModRevision: 12,
		},
	}
	if _, err := ValidateBackupCheckpointAck(ack, request); err != nil {
		t.Fatalf("ValidateBackupCheckpointAck() error = %v", err)
	}
}

// Rationale: a Config restore generation is a stable Controller-owned Config
// identity distinct from the Task attempt carried by the checkpoint envelope.
func TestValidateBackupConfigCheckpointRequiresConfigIdentity(t *testing.T) {
	request := validBackupConfigCheckpointRequest()
	if _, err := ValidateBackupCheckpointRequest(request, request.CheckpointSequence); err != nil {
		t.Fatalf("ValidateBackupCheckpointRequest(config transfer) error = %v", err)
	}
	request.GetConfig().GetTransferCompleted().RestoreGenerationId = request.TaskId
	if _, err := ComputeBackupCheckpointPayloadDigest(request); err == nil {
		t.Fatal("Task identity accepted as restore generation")
	}
}

// Rationale: the durable checkpoint digest binds delivery identity and cannot
// be replayed under another valid assignment identity.
func TestComputeBackupCheckpointPayloadDigestBindsDeliveryIdentity(t *testing.T) {
	request := validBackupCheckpointRequest()
	digest, err := ComputeBackupCheckpointPayloadDigest(request)
	if err != nil {
		t.Fatalf("ComputeBackupCheckpointPayloadDigest() error = %v", err)
	}
	other := proto.Clone(request).(*agentpb.BackupCheckpointRequest)
	other.AssignmentId = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	otherDigest, err := ComputeBackupCheckpointPayloadDigest(other)
	if err != nil {
		t.Fatalf("ComputeBackupCheckpointPayloadDigest(other assignment) error = %v", err)
	}
	repeated, err := ComputeBackupCheckpointPayloadDigest(proto.Clone(request).(*agentpb.BackupCheckpointRequest))
	if err != nil {
		t.Fatalf("ComputeBackupCheckpointPayloadDigest(repeated) error = %v", err)
	}
	if !bytes.Equal(digest, repeated) {
		t.Fatal("checkpoint digest changed for the same typed request")
	}
	if bytes.Equal(digest, otherDigest) {
		t.Fatal("checkpoint digest did not bind assignment identity")
	}
}

// Rationale: upload-completed digesting must bind the returned immutable object
// discriminator rather than only the shared checkpoint delivery envelope.
func TestComputeBackupCheckpointPayloadDigestBindsUploadOutcome(t *testing.T) {
	request := validUploadCompletedCheckpointRequest()
	digest, err := ComputeBackupCheckpointPayloadDigest(request)
	if err != nil {
		t.Fatalf("ComputeBackupCheckpointPayloadDigest(upload completed) error = %v", err)
	}
	request.GetUploadCompleted().GetReturnedObject().GetEtag().Value = "different-etag"
	changed, err := ComputeBackupCheckpointPayloadDigest(request)
	if err != nil {
		t.Fatalf("ComputeBackupCheckpointPayloadDigest(changed upload outcome) error = %v", err)
	}
	if bytes.Equal(digest, changed) {
		t.Fatal("upload-completed digest did not bind immutable object outcome")
	}
}

// Rationale: upload-completed is useful as orphan evidence only when it names
// one canonical point and exact immutable byte/object evidence.
func TestValidateBackupCheckpointRequestRejectsMalformedUploadCompleted(t *testing.T) {
	checks := []struct {
		name   string
		mutate func(*agentpb.BackupCheckpointRequest)
	}{
		{name: "nil typed payload", mutate: func(request *agentpb.BackupCheckpointRequest) {
			request.Checkpoint = &agentpb.BackupCheckpointRequest_UploadVerified{}
		}},
		{name: "bad point", mutate: func(request *agentpb.BackupCheckpointRequest) {
			request.GetUploadCompleted().PointId = "rp_invalid"
		}},
		{name: "zero stored size", mutate: func(request *agentpb.BackupCheckpointRequest) {
			request.GetUploadCompleted().Evidence.StoredSizeBytes = 0
		}},
		{name: "short stored digest", mutate: func(request *agentpb.BackupCheckpointRequest) {
			evidence := request.GetUploadCompleted().Evidence
			evidence.StoredSha256 = evidence.StoredSha256[:sha256.Size-1]
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			request := validUploadCompletedCheckpointRequest()
			check.mutate(request)
			if _, err := ValidateBackupCheckpointRequest(request, request.CheckpointSequence); err == nil {
				t.Fatal("malformed upload-completed checkpoint error = nil")
			}
		})
	}
}

// Rationale: no stale sequence, malformed authority digest, invalid predecessor
// or malformed typed payload may reach the durable checkpoint classifier.
func TestValidateBackupCheckpointRequestRejectsMalformedDelivery(t *testing.T) {
	checks := []struct {
		name   string
		mutate func(*agentpb.BackupCheckpointRequest)
	}{
		{name: "zero sequence", mutate: func(request *agentpb.BackupCheckpointRequest) {
			request.CheckpointSequence = 0
		}},
		{name: "short authority digest", mutate: func(request *agentpb.BackupCheckpointRequest) {
			request.AuthorityDigest = request.AuthorityDigest[:sha256.Size-1]
		}},
		{name: "predecessor on first checkpoint", mutate: func(request *agentpb.BackupCheckpointRequest) {
			request.PrecedingCheckpoint = &agentpb.CheckpointFence{
				AuthorityDigest: append([]byte(nil), request.AuthorityDigest...), DedupeKeyModRevision: 1,
			}
		}},
		{name: "bad point", mutate: func(request *agentpb.BackupCheckpointRequest) {
			request.GetArtifactPrepared().PointId = "rp_invalid"
		}},
		{name: "short artifact digest", mutate: func(request *agentpb.BackupCheckpointRequest) {
			evidence := request.GetArtifactPrepared().Evidence
			evidence.StoredSha256 = evidence.StoredSha256[:sha256.Size-1]
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
// different sequence cannot release the Agent's next action.
func TestValidateBackupCheckpointAckRejectsDifferentIdentity(t *testing.T) {
	request := validBackupCheckpointRequest()
	ack := &agentpb.BackupCheckpointAck{
		TaskId: request.TaskId, AssignmentId: request.AssignmentId,
		StepId: request.StepId, ExecutionId: request.ExecutionId,
		CheckpointSequence: request.CheckpointSequence + 1,
		Committed: &agentpb.CheckpointFence{
			AuthorityDigest: append([]byte(nil), request.AuthorityDigest...), DedupeKeyModRevision: 1,
		},
	}
	if _, err := ValidateBackupCheckpointAck(ack, request); err == nil {
		t.Fatal("ValidateBackupCheckpointAck(different sequence) error = nil")
	}
}

func validBackupPlan(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	step := sealBackupExecutionStep(t, &agentpb.BackupStepAuthority{
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", ExecutionId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		StepDeadlineUnixNano: 1,
		Operation: &agentpb.BackupStepAuthority_Capture{Capture: &agentpb.BackupCaptureAuthority{
			PointId: testBackupPointID,
			Resource: &agentpb.BackupResourceIdentity{
				Kind:       agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ENVIRONMENT,
				ResourceId: testBackupEnvironmentID, Resource: testBackupRevision(3, 3),
			},
			Target: testBackupTarget(testBackupConnectorID, testBackupSourceID, testBackupPointID, true),
			Encryption: &agentpb.BackupEncryptionAuthority{
				Kind:            agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE,
				SecretSlotId:    backupsecret.CurrentAgeIdentitySlotID,
				RecipientSha256: bytes.Repeat([]byte{4}, sha256.Size), SecretSlot: testBackupRevision(4, 4),
				KeyEra: backupUint64(2),
			},
			Source: &agentpb.BackupCaptureAuthority_Config{Config: &agentpb.BackupConfigCaptureAuthority{
				EnvironmentId: testBackupEnvironmentID, Content: testBackupConfigContent(),
				MetadataSnapshotRevision: 8, MetadataEntryCount: 1, MetadataProtoBytes: 128,
			}},
		}},
	})
	return &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: testPlanID,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP, TargetId: testBackupEnvironmentID,
		BackupScope: testBackupScope(), Steps: []*agentpb.ExecutionStep{step},
	}
}

func validBackupPrunePlan(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	return &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE, TargetId: testBackupEnvironmentID,
		BackupScope: testBackupScope(),
		Steps: []*agentpb.ExecutionStep{
			backupPruneStep(
				t,
				"step_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				"01ARZ3NDEKTSV4RRFFQ69G5FAV",
				1,
				testBackupPointID,
				testBackupSourceID,
				testBackupConnectorID,
				"https://objects.example.test",
				"production/",
				true,
			),
			backupPruneStep(t, "step_01ARZ3NDEKTSV4RRFFQ69G5FAW", "01ARZ3NDEKTSV4RRFFQ69G5FAW", 2,
				"rp_01ARZ3NDEKTSV4RRFFQ69G5FAW", "spt_01ARZ3NDEKTSV4RRFFQ69G5FAW",
				"con_01ARZ3NDEKTSV4RRFFQ69G5FAW", "https://archive.example.test", "archive/", false),
		},
	}
}

func backupPruneStep(t *testing.T, stepID, executionID string, ordinal uint32, pointID, sourceID,
	connectorID, endpoint, prefix string, pathStyle bool,
) *agentpb.ExecutionStep {
	t.Helper()
	connector := testBackupConnector(connectorID, endpoint, prefix, pathStyle)
	digest := bytes.Repeat([]byte{byte(ordinal)}, sha256.Size)
	authority := &agentpb.BackupStepAuthority{
		StepId: stepID, ExecutionId: executionID, StepDeadlineUnixNano: 2,
		Operation: &agentpb.BackupStepAuthority_Prune{Prune: &agentpb.BackupPruneAuthority{
			RetentionPolicy: testBackupRevision(20, 20),
			Objects: []*agentpb.BackupPruneObject{{
				Ordinal: ordinal, PointId: pointID, Point: testBackupRevision(int64(30+ordinal), byte(30+ordinal)),
				Evidence: &agentpb.BackupArtifactEvidence{
					SourceSizeBytes: 4096, SourceSha256: append([]byte(nil), digest...),
					StoredSizeBytes: 4096, StoredSha256: append([]byte(nil), digest...),
				},
				Object: &agentpb.BackupObjectIdentity{
					Connector: connector, Bucket: "groundplane-backups",
					ObjectKey:     prefix + testBackupEnvironmentID + "/" + sourceID + "/" + pointID + "/artifact.bin",
					Discriminator: &agentpb.BackupObjectIdentity_Etag{Etag: &agentpb.BackupS3ETag{Value: "etag"}},
				},
				MetadataCount: 10, MetadataSha256: bytes.Repeat([]byte{9}, sha256.Size),
			}},
		}},
	}
	return sealBackupExecutionStep(t, authority)
}

func sealBackupExecutionStep(t *testing.T, authority *agentpb.BackupStepAuthority) *agentpb.ExecutionStep {
	t.Helper()
	sealed, err := SealBackupStepAuthority(authority)
	if err != nil {
		t.Fatalf("SealBackupStepAuthority() error = %v", err)
	}
	return &agentpb.ExecutionStep{
		StepId: sealed.StepId, TimeoutSeconds: MaximumBackupPruneStepTimeoutSeconds,
		Payload: &agentpb.ExecutionStep_BackupStep{BackupStep: sealed},
	}
}

func resealBackupExecutionStep(t *testing.T, step *agentpb.ExecutionStep) {
	t.Helper()
	authority := step.GetBackupStep()
	authority.StepDigest = nil
	sealed, err := SealBackupStepAuthority(authority)
	if err != nil {
		return
	}
	step.Payload = &agentpb.ExecutionStep_BackupStep{BackupStep: sealed}
}

func testBackupScope() *agentpb.BackupPlanScope {
	return &agentpb.BackupPlanScope{
		ProjectId: testBackupProjectID, Project: testBackupRevision(1, 1),
		EnvironmentId: testBackupEnvironmentID, Environment: testBackupRevision(2, 2), TaskAttempt: 1,
	}
}

func validBackupServiceFactFixture(serviceID string) *agentpb.BackupServiceFact {
	return &agentpb.BackupServiceFact{
		ServiceId: serviceID, CurrentName: "application", Service: testBackupRevision(5, 5),
		Compose: testBackupRevision(6, 6),
		PriorRuntimeIntent: &agentpb.BackupPriorRuntimeIntent{
			Kind:   agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING,
			Intent: testBackupRevision(7, 7),
		},
		RequiredLabelCount: 1, RequiredLabelsSha256: bytes.Repeat([]byte{8}, sha256.Size),
		LocalImageIdSha256: bytes.Repeat([]byte{9}, sha256.Size),
	}
}

func testBackupTarget(connectorID, sourceID, pointID string, pathStyle bool) *agentpb.BackupObjectTarget {
	connector := testBackupConnector(connectorID, "https://objects.example.test", "production/", pathStyle)
	return &agentpb.BackupObjectTarget{
		Connector: connector, Bucket: "groundplane-backups",
		ObjectKey: connector.Prefix + testBackupEnvironmentID + "/" + sourceID + "/" + pointID + "/artifact.bin",
	}
}

func testBackupConnector(connectorID, endpoint, prefix string, pathStyle bool) *agentpb.BackupConnectorAuthority {
	return &agentpb.BackupConnectorAuthority{
		ConnectorId: connectorID, Connector: testBackupRevision(10, 10),
		CanonicalEndpointUrl: endpoint, Region: "auto", PathStyle: backupBool(pathStyle), Prefix: prefix,
		AccessKeySlotId: backupsecret.AccessKeySlotID, SecretKeySlotId: backupsecret.SecretKeySlotID,
		AccessKeySlot: testBackupRevision(11, 11), SecretKeySlot: testBackupRevision(12, 12),
	}
}

func testBackupRevision(revision int64, fill byte) *agentpb.RevisionDigest {
	return &agentpb.RevisionDigest{ModRevision: revision, Sha256: bytes.Repeat([]byte{fill}, sha256.Size)}
}

func testBackupConfigContent() *agentpb.BackupConfigContentAuthority {
	return &agentpb.BackupConfigContentAuthority{
		ManifestSha256: bytes.Repeat([]byte{13}, sha256.Size), EntryCount: 1,
		TotalSelectedValueBytes: 32, ManifestSizeBytes: 128, SourceSizeBytes: 1536,
		MetadataSnapshotSha256: bytes.Repeat([]byte{14}, sha256.Size),
	}
}

func validBackupCheckpointRequest() *agentpb.BackupCheckpointRequest {
	digest := bytes.Repeat([]byte{21}, sha256.Size)
	sameInode := true
	return &agentpb.BackupCheckpointRequest{
		TaskId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", ExecutionId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		CheckpointSequence: 1, AuthorityDigest: bytes.Repeat([]byte{20}, sha256.Size),
		Checkpoint: &agentpb.BackupCheckpointRequest_ArtifactPrepared{ArtifactPrepared: &agentpb.BackupArtifactPrepared{
			PointId: testBackupPointID,
			Evidence: &agentpb.BackupArtifactEvidence{
				SourceSizeBytes: 1536, SourceSha256: append([]byte(nil), digest...),
				StoredSizeBytes: 1536, StoredSha256: append([]byte(nil), digest...),
			},
			Finals: &agentpb.BackupStagingFinals{
				SourceRelativeName: BackupSourceStagingFinal, StoredRelativeName: BackupSourceStagingFinal,
				SameInode: &sameInode,
			},
			Archive: &agentpb.BackupArtifactPrepared_Volume{Volume: &agentpb.BackupVolumeArchiveEvidence{
				EntryCount: 1, ContentManifestSha256: bytes.Repeat([]byte{22}, sha256.Size),
				FullTreeSha256: bytes.Repeat([]byte{23}, sha256.Size), SourceSizeBytes: 1536,
			}},
		}},
	}
}

func validUploadCompletedCheckpointRequest() *agentpb.BackupCheckpointRequest {
	target := testBackupTarget(testBackupConnectorID, testBackupSourceID, testBackupPointID, true)
	digest := bytes.Repeat([]byte{24}, sha256.Size)
	return &agentpb.BackupCheckpointRequest{
		TaskId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", ExecutionId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		CheckpointSequence: 1, AuthorityDigest: bytes.Repeat([]byte{20}, sha256.Size),
		Checkpoint: &agentpb.BackupCheckpointRequest_UploadCompleted{UploadCompleted: &agentpb.BackupUploadCompleted{
			PointId: testBackupPointID,
			Evidence: &agentpb.BackupArtifactEvidence{
				SourceSizeBytes: 4096, SourceSha256: append([]byte(nil), digest...),
				StoredSizeBytes: 4096, StoredSha256: append([]byte(nil), digest...),
			},
			Target: target,
			Outcome: &agentpb.BackupUploadCompleted_ReturnedObject{ReturnedObject: &agentpb.BackupObjectIdentity{
				Connector: proto.Clone(target.Connector).(*agentpb.BackupConnectorAuthority), Bucket: target.Bucket,
				ObjectKey:     target.ObjectKey,
				Discriminator: &agentpb.BackupObjectIdentity_Etag{Etag: &agentpb.BackupS3ETag{Value: "etag"}},
			}},
			MetadataCount: 10, MetadataSha256: bytes.Repeat([]byte{25}, sha256.Size),
		}},
	}
}

func validBackupConfigCheckpointRequest() *agentpb.BackupCheckpointRequest {
	return &agentpb.BackupCheckpointRequest{
		TaskId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", ExecutionId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		CheckpointSequence: 1, AuthorityDigest: bytes.Repeat([]byte{20}, sha256.Size),
		Checkpoint: &agentpb.BackupCheckpointRequest_Config{Config: &agentpb.BackupConfigCheckpoint{
			Checkpoint: &agentpb.BackupConfigCheckpoint_TransferCompleted{
				TransferCompleted: &agentpb.BackupConfigTransferCompleted{
					RestoreGenerationId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV", Content: testBackupConfigContent(),
					CommittedRecordCount: 4, ValueChainSha256: bytes.Repeat([]byte{26}, sha256.Size),
					TransferTranscriptSha256: bytes.Repeat([]byte{27}, sha256.Size), RenderGeneration: 1,
				},
			},
		}},
	}
}

func backupBool(value bool) *bool       { return &value }
func backupUint64(value uint64) *uint64 { return &value }
